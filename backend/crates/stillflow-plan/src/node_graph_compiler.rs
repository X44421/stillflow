use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use stillflow_core::{
    CastFailurePolicy as NodeCastFailurePolicy, ColumnId, Expr, LogicalField, LogicalSchema,
    LogicalType, NodeEdge, NodeGraph, NodeGraphError, NodeGraphErrorCode, NodeId, NodeRegistry,
    ValidatedNodeConfig, MAX_EXPR_DEPTH, MAX_EXPR_NODES, MAX_METADATA_BYTES, MAX_NESTING_DEPTH,
    MAX_NODES,
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    semantics, CastFailurePolicy, LogicalPlan, PlanError, PlanFingerprint, PlanNode, PlanNodeId,
    PlanNodeKind, Rule,
};
use semantics::SemanticError;

pub const NODE_GRAPH_COMPILER_VERSION: &str = "ng-nodegraph-compiler-v1";
pub const MAX_DIAGNOSTICS: usize = 64;
pub const MAX_DIAGNOSTIC_BYTES: usize = 1024;
pub const MAX_COMPILE_WORK: usize = 2_000_000;

/// Authorized source identity and schema. No connector or credential crosses
/// the graph compiler boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedSourceContext {
    pub source_asset_id: Uuid,
    pub schema: LogicalSchema,
}

impl AuthorizedSourceContext {
    pub fn new(
        source_asset_id: Uuid,
        schema: LogicalSchema,
    ) -> Result<Self, NodeGraphCompileError> {
        if source_asset_id.is_nil() {
            return Err(NodeGraphCompileError::new(
                NodeGraphErrorCode::SourceBinding,
                None,
                "authorized source asset is nil",
            ));
        }
        schema.validate().map_err(|_| {
            NodeGraphCompileError::new(
                NodeGraphErrorCode::InvalidConfig,
                None,
                "authorized source schema is invalid",
            )
        })?;
        Ok(Self {
            source_asset_id,
            schema,
        })
    }
}

/// Preview selection only requests a mapping to an emitted plan node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompileTarget {
    Execution,
    Preview(NodeId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileDiagnostic {
    pub code: NodeGraphErrorCode,
    pub node_id: Option<NodeId>,
    pub column_id: Option<ColumnId>,
    pub field_path: Option<String>,
    pub expected: Option<String>,
    pub actual: Option<String>,
    pub message: String,
}

/// The bounded safe-location payload of NX-C0 §7.1, boxed so the error
/// stays small on the compile path's hot `Result`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ErrorLocation {
    pub column_id: Option<ColumnId>,
    pub field_path: Option<String>,
    pub expected: Option<String>,
    pub actual: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}: {message}")]
pub struct NodeGraphCompileError {
    code: NodeGraphErrorCode,
    node_id: Option<NodeId>,
    location: Option<Box<ErrorLocation>>,
    message: String,
}

impl NodeGraphCompileError {
    pub fn new(
        code: NodeGraphErrorCode,
        node_id: Option<NodeId>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            node_id,
            location: None,
            message: message.into(),
        }
    }

    /// Attaches the bounded safe-location fields of NX-C0 §7.1. Locations
    /// name structural paths and identifiers; they never carry caller
    /// values (§7.3).
    pub fn with_location(
        mut self,
        column_id: Option<ColumnId>,
        field_path: Option<String>,
        expected: Option<String>,
        actual: Option<String>,
    ) -> Self {
        if column_id.is_none() && field_path.is_none() && expected.is_none() && actual.is_none() {
            return self;
        }
        self.location = Some(Box::new(ErrorLocation {
            column_id,
            field_path,
            expected,
            actual,
        }));
        self
    }

    pub fn location(&self) -> Option<&ErrorLocation> {
        self.location.as_deref()
    }

    pub fn column_id(&self) -> Option<ColumnId> {
        self.location
            .as_ref()
            .and_then(|location| location.column_id)
    }

    pub fn field_path(&self) -> Option<&str> {
        self.location
            .as_ref()
            .and_then(|location| location.field_path.as_deref())
    }

    pub fn expected(&self) -> Option<&str> {
        self.location
            .as_ref()
            .and_then(|location| location.expected.as_deref())
    }

    pub fn actual(&self) -> Option<&str> {
        self.location
            .as_ref()
            .and_then(|location| location.actual.as_deref())
    }

    pub const fn code(&self) -> NodeGraphErrorCode {
        self.code
    }

    pub const fn node_id(&self) -> Option<NodeId> {
        self.node_id
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// A compiled graph contains the existing logical plan plus schema and
/// product-node mappings. The plan remains the only execution authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledNodeGraph {
    pub plan: LogicalPlan,
    pub output_schema: LogicalSchema,
    pub node_plan_ids: BTreeMap<NodeId, PlanNodeId>,
    pub node_schemas: BTreeMap<NodeId, LogicalSchema>,
    pub diagnostics: Vec<CompileDiagnostic>,
}

impl CompiledNodeGraph {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, PlanError> {
        self.plan.canonical_bytes()
    }

    pub fn fingerprint(&self) -> Result<PlanFingerprint, PlanError> {
        self.plan.fingerprint()
    }

    pub fn preview_plan_node_id(
        &self,
        target: NodeId,
    ) -> Result<PlanNodeId, NodeGraphCompileError> {
        let plan_id = self.node_plan_ids.get(&target).copied().ok_or_else(|| {
            NodeGraphCompileError::new(
                NodeGraphErrorCode::UnsupportedTarget,
                Some(target),
                "preview target is not present in the compiled graph",
            )
        })?;
        if plan_id == self.plan.root {
            return Err(NodeGraphCompileError::new(
                NodeGraphErrorCode::UnsupportedTarget,
                Some(target),
                "materialize node is not a preview target",
            ));
        }
        Ok(plan_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeGraphCompiler {
    registry: NodeRegistry,
}

impl Default for NodeGraphCompiler {
    fn default() -> Self {
        Self::new(NodeRegistry::new())
    }
}

impl NodeGraphCompiler {
    pub fn new(registry: NodeRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &NodeRegistry {
        &self.registry
    }

    pub fn compile(
        &self,
        graph: &NodeGraph,
        source: &AuthorizedSourceContext,
        target: CompileTarget,
    ) -> Result<CompiledNodeGraph, NodeGraphCompileError> {
        check_shape_work(graph)?;
        source.schema.validate().map_err(|_| {
            NodeGraphCompileError::new(
                NodeGraphErrorCode::InvalidConfig,
                None,
                "authorized source schema is invalid",
            )
        })?;
        let configs = graph
            .validated_configs(&self.registry)
            .map_err(map_graph_error)?;
        check_compile_work(graph, &configs, Some(&source.schema))?;
        let path = unique_path(graph)?;

        let (source_asset_id, projection) = match configs.get(&graph.source_node_id) {
            Some(ValidatedNodeConfig::Source {
                source_asset_id,
                projection,
            }) => (*source_asset_id, projection.clone()),
            _ => {
                return Err(NodeGraphCompileError::new(
                    NodeGraphErrorCode::InvalidTopology,
                    Some(graph.source_node_id),
                    "declared source node did not resolve to a source config",
                ));
            }
        };
        if source_asset_id != source.source_asset_id {
            return Err(NodeGraphCompileError::new(
                NodeGraphErrorCode::SourceBinding,
                Some(graph.source_node_id),
                "graph source asset is not the authorized source asset",
            ));
        }
        if let CompileTarget::Preview(node) = target {
            if node == graph.output_node_id || !configs.contains_key(&node) {
                return Err(NodeGraphCompileError::new(
                    NodeGraphErrorCode::UnsupportedTarget,
                    Some(node),
                    "preview target is not an emitted non-materialize node",
                ));
            }
        }

        let node_plan_ids: BTreeMap<_, _> = path
            .iter()
            .copied()
            .map(|node_id| (node_id, PlanNodeId::from_uuid(node_id.as_uuid())))
            .collect();
        let mut plan_nodes = BTreeMap::new();
        let mut node_schemas = BTreeMap::new();
        let mut working_schema =
            project_source_schema(&source.schema, projection.as_deref(), graph.source_node_id)?;
        let mut previous = None;

        for node_id in &path {
            let plan_id = node_plan_ids[node_id];
            let config = configs.get(node_id).ok_or_else(|| {
                NodeGraphCompileError::new(
                    NodeGraphErrorCode::Internal,
                    Some(*node_id),
                    "validated node config is absent",
                )
            })?;
            let (kind, next_schema) = if *node_id == graph.source_node_id {
                (
                    PlanNodeKind::Scan {
                        source_asset_id,
                        projection: working_schema.fields.iter().map(|field| field.id).collect(),
                        predicate: None,
                    },
                    working_schema.clone(),
                )
            } else if *node_id == graph.output_node_id {
                let ValidatedNodeConfig::Output { output_label } = config else {
                    return Err(NodeGraphCompileError::new(
                        NodeGraphErrorCode::InvalidTopology,
                        Some(*node_id),
                        "declared output node did not resolve to an output config",
                    ));
                };
                (
                    PlanNodeKind::Materialize {
                        output_label: output_label.clone(),
                    },
                    working_schema.clone(),
                )
            } else {
                compile_transform(*node_id, config, &working_schema)?
            };
            plan_nodes.insert(plan_id, PlanNode::new(kind, previous.into_iter().collect()));
            node_schemas.insert(*node_id, next_schema.clone());
            working_schema = next_schema;
            previous = Some(plan_id);
        }

        let plan = LogicalPlan::new(node_plan_ids[&graph.output_node_id], plan_nodes)
            .map_err(plan_error)?;
        plan.canonical_bytes().map_err(plan_error)?;
        plan.fingerprint().map_err(plan_error)?;

        let result = CompiledNodeGraph {
            plan,
            output_schema: working_schema,
            node_plan_ids,
            node_schemas,
            diagnostics: Vec::new(),
        };
        if let CompileTarget::Preview(node) = target {
            result.preview_plan_node_id(node)?;
        }
        Ok(result)
    }
}

/// Pure-graph validation (NX-C0 §8.1 stage 4): structural checks, registry
/// lookup, config constraints, ports, topology, and the graph-to-request
/// source binding — everything that needs no source schema and no connector.
/// The API runs this before resolving the authorized source schema so a
/// decodable graph that fails here performs zero connector calls.
pub fn validate_node_graph(
    graph: &NodeGraph,
    registry: &NodeRegistry,
    source_asset_id: Uuid,
) -> Result<(), NodeGraphCompileError> {
    check_shape_work(graph)?;
    let configs = graph.validated_configs(registry).map_err(map_graph_error)?;
    check_compile_work(graph, &configs, None)?;
    unique_path(graph)?;
    match configs.get(&graph.source_node_id) {
        Some(ValidatedNodeConfig::Source {
            source_asset_id: bound,
            ..
        }) if *bound == source_asset_id => Ok(()),
        Some(ValidatedNodeConfig::Source { .. }) => Err(NodeGraphCompileError::new(
            NodeGraphErrorCode::SourceBinding,
            Some(graph.source_node_id),
            "graph source asset is not the authorized source asset",
        )),
        _ => Err(NodeGraphCompileError::new(
            NodeGraphErrorCode::InvalidTopology,
            Some(graph.source_node_id),
            "declared source node did not resolve to a source config",
        )),
    }
}

pub fn compile_node_graph(
    graph: &NodeGraph,
    registry: &NodeRegistry,
    source: &AuthorizedSourceContext,
    target: CompileTarget,
) -> Result<CompiledNodeGraph, NodeGraphCompileError> {
    NodeGraphCompiler::new(registry.clone()).compile(graph, source, target)
}

fn check_shape_work(graph: &NodeGraph) -> Result<(), NodeGraphCompileError> {
    if graph.nodes.len() > MAX_NODES {
        return Err(NodeGraphCompileError::new(
            NodeGraphErrorCode::LimitNodes,
            None,
            "graph node count exceeds the contract limit",
        ));
    }
    let metadata_entries = graph
        .metadata
        .len()
        .checked_add(
            graph
                .nodes
                .iter()
                .try_fold(0_usize, |count, node| {
                    count.checked_add(node.metadata.len())
                })
                .ok_or_else(limit_error)?,
        )
        .ok_or_else(limit_error)?;
    if metadata_entries.checked_mul(16).is_none() || metadata_entries > MAX_METADATA_BYTES {
        return Err(NodeGraphCompileError::new(
            NodeGraphErrorCode::LimitMetadataBytes,
            None,
            "graph metadata limit exceeded",
        ));
    }
    let mut work = graph
        .nodes
        .len()
        .checked_add(graph.edges.len())
        .ok_or_else(limit_error)?
        .checked_add(metadata_entries.checked_mul(16).ok_or_else(limit_error)?)
        .ok_or_else(limit_error)?;
    for node in &graph.nodes {
        let bytes = serde_json::to_vec(node.config()).map_err(|_| {
            NodeGraphCompileError::new(
                NodeGraphErrorCode::InvalidConfig,
                Some(node.id()),
                "node config could not be serialized",
            )
        })?;
        if bytes.len() > stillflow_core::MAX_CONFIG_BYTES {
            return Err(NodeGraphCompileError::new(
                NodeGraphErrorCode::LimitConfigBytes,
                Some(node.id()),
                "node config exceeds the contract limit",
            ));
        }
        work = work.checked_add(bytes.len()).ok_or_else(limit_error)?;
    }
    if work > MAX_COMPILE_WORK {
        return Err(limit_error());
    }
    Ok(())
}

fn check_compile_work(
    graph: &NodeGraph,
    configs: &BTreeMap<NodeId, ValidatedNodeConfig>,
    schema: Option<&LogicalSchema>,
) -> Result<(), NodeGraphCompileError> {
    let metadata_entries = graph
        .metadata
        .len()
        .checked_add(
            graph
                .nodes
                .iter()
                .try_fold(0_usize, |count, node| {
                    count.checked_add(node.metadata.len())
                })
                .ok_or_else(limit_error)?,
        )
        .ok_or_else(limit_error)?;
    let config_bytes = graph.nodes.iter().try_fold(0_usize, |total, node| {
        serde_json::to_vec(node.config())
            .map_err(|_| limit_error())
            .and_then(|bytes| total.checked_add(bytes.len()).ok_or_else(limit_error))
    })?;
    let expression_nodes = configs.values().try_fold(0_usize, |total, config| {
        let count = match config {
            ValidatedNodeConfig::Filter { predicate }
            | ValidatedNodeConfig::DeriveColumn {
                expression: predicate,
                ..
            } => expression_shape(predicate)?.0,
            _ => 0,
        };
        total.checked_add(count).ok_or_else(limit_error)
    })?;
    let schema_fields = match schema {
        Some(schema) => schema_field_count(schema)?,
        None => 0,
    };
    let work = graph
        .nodes
        .len()
        .checked_add(graph.edges.len())
        .and_then(|value| value.checked_add(config_bytes))
        .and_then(|value| value.checked_add(expression_nodes.checked_mul(4)?))
        .and_then(|value| value.checked_add(schema_fields.checked_mul(4)?))
        .and_then(|value| value.checked_add(metadata_entries.checked_mul(16)?))
        .ok_or_else(limit_error)?;
    if work > MAX_COMPILE_WORK {
        return Err(limit_error());
    }
    Ok(())
}

fn expression_shape(expr: &Expr) -> Result<(usize, usize), NodeGraphCompileError> {
    let mut count = 0_usize;
    let mut max_depth = 0_usize;
    let mut pending = vec![(expr, 1_usize)];
    while let Some((current, depth)) = pending.pop() {
        count = count.checked_add(1).ok_or_else(limit_error)?;
        max_depth = max_depth.max(depth);
        if count > MAX_EXPR_NODES || max_depth > MAX_EXPR_DEPTH {
            return Err(NodeGraphCompileError::new(
                NodeGraphErrorCode::LimitNestingDepth,
                None,
                "expression exceeds the contract limit",
            ));
        }
        match current {
            Expr::Column(_) | Expr::Literal(_) => {}
            Expr::Unary { expression, .. }
            | Expr::IsNull { expression, .. }
            | Expr::Cast { expression, .. } => pending.push((expression, depth + 1)),
            Expr::Binary { left, right, .. } => {
                pending.push((left, depth + 1));
                pending.push((right, depth + 1));
            }
            Expr::Coalesce { expressions } => {
                if expressions.is_empty() {
                    return Err(invalid_config_any("coalesce expression is empty"));
                }
                for expression in expressions {
                    pending.push((expression, depth + 1));
                }
            }
        }
    }
    Ok((count, max_depth))
}

fn schema_field_count(schema: &LogicalSchema) -> Result<usize, NodeGraphCompileError> {
    let mut count = 0_usize;
    let mut pending: Vec<(&[LogicalField], usize)> = vec![(&schema.fields, 1)];
    while let Some((fields, depth)) = pending.pop() {
        if depth > MAX_NESTING_DEPTH {
            return Err(NodeGraphCompileError::new(
                NodeGraphErrorCode::LimitNestingDepth,
                None,
                "schema nesting exceeds the contract limit",
            ));
        }
        count = count.checked_add(fields.len()).ok_or_else(limit_error)?;
        for field in fields {
            match &field.data_type {
                LogicalType::Struct(nested) => pending.push((nested, depth + 1)),
                LogicalType::List(element) => {
                    let mut current = element.as_ref();
                    let mut current_depth = depth + 1;
                    loop {
                        if current_depth > MAX_NESTING_DEPTH {
                            return Err(NodeGraphCompileError::new(
                                NodeGraphErrorCode::LimitNestingDepth,
                                None,
                                "schema nesting exceeds the contract limit",
                            ));
                        }
                        match current {
                            LogicalType::List(next) => {
                                current = next;
                                current_depth += 1;
                            }
                            LogicalType::Struct(nested) => {
                                pending.push((nested, current_depth));
                                break;
                            }
                            _ => break,
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(count)
}

fn unique_path(graph: &NodeGraph) -> Result<Vec<NodeId>, NodeGraphCompileError> {
    let mut outgoing = BTreeMap::new();
    // Edge faults are reported by ascending endpoint tuple, never by caller
    // array order (NX-C0 §7.2, R-8).
    let mut ordered_edges: Vec<&NodeEdge> = graph.edges.iter().collect();
    ordered_edges.sort_by_key(|edge| {
        (
            edge.from.node_id,
            edge.from.port.clone(),
            edge.to.node_id,
            edge.to.port.clone(),
        )
    });
    for edge in ordered_edges {
        if outgoing
            .insert(edge.from.node_id, edge.to.node_id)
            .is_some()
        {
            return Err(NodeGraphCompileError::new(
                NodeGraphErrorCode::InvalidTopology,
                Some(edge.from.node_id),
                "graph has more than one outgoing edge",
            ));
        }
    }
    let mut path = Vec::with_capacity(graph.nodes.len());
    let mut visited = BTreeSet::new();
    let mut current = graph.source_node_id;
    loop {
        if !visited.insert(current) {
            return Err(NodeGraphCompileError::new(
                NodeGraphErrorCode::InvalidTopology,
                Some(current),
                "graph path contains a cycle",
            ));
        }
        path.push(current);
        if current == graph.output_node_id {
            break;
        }
        current = outgoing.get(&current).copied().ok_or_else(|| {
            NodeGraphCompileError::new(
                NodeGraphErrorCode::InvalidTopology,
                Some(current),
                "graph path terminates before output",
            )
        })?;
    }
    if path.len() != graph.nodes.len() {
        return Err(NodeGraphCompileError::new(
            NodeGraphErrorCode::InvalidTopology,
            None,
            "graph contains disconnected nodes",
        ));
    }
    Ok(path)
}

fn project_source_schema(
    schema: &LogicalSchema,
    projection: Option<&[ColumnId]>,
    node_id: NodeId,
) -> Result<LogicalSchema, NodeGraphCompileError> {
    let columns: Vec<ColumnId> = projection
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| schema.fields.iter().map(|field| field.id).collect());
    semantics::project_effect(&schema.clone(), &columns).map_err(|error| {
        NodeGraphCompileError::new(error.code(), Some(node_id), error.compile_message())
    })
}

fn product_rule(config: &ValidatedNodeConfig) -> Option<Rule> {
    match config {
        ValidatedNodeConfig::Rename { column, to } => Some(Rule::Rename {
            column: *column,
            to: to.clone(),
        }),
        ValidatedNodeConfig::Trim { column } => Some(Rule::Trim { column: *column }),
        ValidatedNodeConfig::Cast {
            column,
            data_type,
            on_failure,
        } => Some(Rule::Cast {
            column: *column,
            data_type: data_type.clone(),
            on_failure: match on_failure {
                NodeCastFailurePolicy::Error => CastFailurePolicy::Error,
                NodeCastFailurePolicy::SetNull => CastFailurePolicy::SetNull,
            },
        }),
        ValidatedNodeConfig::ReplaceLiteral { column, from, to } => Some(Rule::ReplaceLiteral {
            column: *column,
            from: from.clone(),
            to: to.clone(),
        }),
        ValidatedNodeConfig::FillNull { column, value } => Some(Rule::FillNull {
            column: *column,
            value: value.clone(),
        }),
        ValidatedNodeConfig::DropColumn { column } => Some(Rule::DropColumn { column: *column }),
        ValidatedNodeConfig::DeriveColumn {
            id,
            name,
            data_type,
            nullable,
            expression,
        } => Some(Rule::DeriveColumn {
            id: *id,
            name: name.clone(),
            data_type: data_type.clone(),
            nullable: *nullable,
            expression: expression.clone(),
        }),
        ValidatedNodeConfig::Select { .. } | ValidatedNodeConfig::Filter { .. } => None,
        ValidatedNodeConfig::Source { .. } | ValidatedNodeConfig::Output { .. } => None,
    }
}

fn compile_transform(
    node_id: NodeId,
    config: &ValidatedNodeConfig,
    schema: &LogicalSchema,
) -> Result<(PlanNodeKind, LogicalSchema), NodeGraphCompileError> {
    if let Some(rule) = product_rule(config) {
        let next_schema = semantics::rule_effect(schema, &rule)
            .map_err(|error| semantic_error(error, node_id))?;
        return Ok((PlanNodeKind::ApplyRules { rules: vec![rule] }, next_schema));
    }
    match config {
        ValidatedNodeConfig::Select { columns } => {
            let next_schema = semantics::project_effect(schema, columns)
                .map_err(|error| semantic_error(error, node_id))?;
            Ok((
                PlanNodeKind::Project {
                    columns: columns.clone(),
                },
                next_schema,
            ))
        }
        ValidatedNodeConfig::Filter { predicate } => {
            let analysis = semantics::analyze_expr(predicate, schema)
                .map_err(|error| semantic_error(error, node_id))?;
            if analysis.data_type != LogicalType::Boolean {
                return Err(type_error(node_id, "filter predicate must be boolean"));
            }
            Ok((
                PlanNodeKind::Filter {
                    predicate: predicate.clone(),
                },
                schema.clone(),
            ))
        }
        ValidatedNodeConfig::Source { .. } | ValidatedNodeConfig::Output { .. } => {
            Err(NodeGraphCompileError::new(
                NodeGraphErrorCode::InvalidTopology,
                Some(node_id),
                "source and output configs are not transform nodes",
            ))
        }
        // Every other validated config produces a rule and was handled above.
        _ => unreachable!("product rule covered all rule-producing configs"),
    }
}

fn semantic_error(error: SemanticError, node_id: NodeId) -> NodeGraphCompileError {
    NodeGraphCompileError::new(error.code(), Some(node_id), error.compile_message())
}

fn map_graph_error(error: NodeGraphError) -> NodeGraphCompileError {
    let message = match error.code() {
        NodeGraphErrorCode::UnsupportedGraphVersion => "graph version is not supported",
        NodeGraphErrorCode::UnknownNodeType => "node type is not registered",
        NodeGraphErrorCode::UnsupportedConfigVersion => "node config version is not supported",
        NodeGraphErrorCode::InvalidConfig => "node config is invalid",
        NodeGraphErrorCode::InvalidPort => "node port is invalid",
        NodeGraphErrorCode::InvalidTopology => "graph topology is not authorized",
        NodeGraphErrorCode::SourceBinding => "graph source binding is invalid",
        NodeGraphErrorCode::LimitGraphBytes => "graph byte limit exceeded",
        NodeGraphErrorCode::LimitNodes => "graph node limit exceeded",
        NodeGraphErrorCode::LimitEdges => "graph edge limit exceeded",
        NodeGraphErrorCode::LimitConfigBytes => "graph config byte limit exceeded",
        NodeGraphErrorCode::LimitMetadataBytes => "graph metadata byte limit exceeded",
        NodeGraphErrorCode::LimitStringBytes => "graph string limit exceeded",
        NodeGraphErrorCode::LimitNestingDepth => "graph nesting limit exceeded",
        NodeGraphErrorCode::LimitRulesPerNode => "node rule limit exceeded",
        NodeGraphErrorCode::LimitRules => "graph rule limit exceeded",
        NodeGraphErrorCode::LimitCompileWork => "compile work limit exceeded",
        _ => "graph validation failed",
    };
    NodeGraphCompileError::new(error.code(), error.node_id(), message).with_location(
        None,
        error.field_path().map(str::to_owned),
        None,
        None,
    )
}

fn plan_error(_: PlanError) -> NodeGraphCompileError {
    NodeGraphCompileError::new(
        NodeGraphErrorCode::PlanInvalid,
        None,
        "compiled logical plan failed validation or canonicalization",
    )
}

fn invalid_config_any(message: &'static str) -> NodeGraphCompileError {
    NodeGraphCompileError::new(NodeGraphErrorCode::InvalidConfig, None, message)
}

fn type_error(node_id: NodeId, message: &'static str) -> NodeGraphCompileError {
    NodeGraphCompileError::new(NodeGraphErrorCode::IncompatibleType, Some(node_id), message)
}

fn limit_error() -> NodeGraphCompileError {
    NodeGraphCompileError::new(
        NodeGraphErrorCode::LimitCompileWork,
        None,
        "compile work exceeds the contract limit",
    )
}

impl fmt::Display for CompileTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execution => formatter.write_str("execution"),
            Self::Preview(_) => formatter.write_str("preview"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Serialize;
    use serde_json::{json, Value};
    use stillflow_core::{
        BinaryOperator, LogicalField, NodeConfig, NodeEdge, NodePort, PortId, ScalarValue,
    };

    use super::*;

    fn uuid(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn node(value: u128) -> NodeId {
        NodeId::from_uuid(uuid(value))
    }

    fn column(value: u128) -> ColumnId {
        ColumnId::from_uuid(uuid(value))
    }

    fn serialized<T: Serialize>(value: &T) -> Value {
        serde_json::to_value(value).expect("serializable test config")
    }

    fn config(id: u128, type_id: &str, value: Value) -> NodeConfig {
        NodeConfig::new(node(id), type_id, 1, value, BTreeMap::new()).expect("valid node config")
    }

    fn edge(from: u128, to: u128) -> NodeEdge {
        NodeEdge {
            from: NodePort {
                node_id: node(from),
                port: PortId::new("out").expect("port"),
            },
            to: NodePort {
                node_id: node(to),
                port: PortId::new("in").expect("port"),
            },
        }
    }

    fn graph(nodes: Vec<NodeConfig>, edges: Vec<NodeEdge>) -> NodeGraph {
        NodeGraph::new(uuid(900), node(1), node(5), nodes, edges, BTreeMap::new())
            .expect("basic graph")
    }

    fn source_schema() -> LogicalSchema {
        LogicalSchema::new(vec![
            LogicalField::new(column(101), "name", LogicalType::Utf8, true).expect("field"),
            LogicalField::new(column(102), "age", LogicalType::Int64, true).expect("field"),
            LogicalField::new(column(103), "active", LogicalType::Boolean, false).expect("field"),
        ])
        .expect("schema")
    }

    fn source() -> AuthorizedSourceContext {
        AuthorizedSourceContext::new(uuid(700), source_schema()).expect("source")
    }

    fn source_config() -> NodeConfig {
        config(
            1,
            "stillflow.node.source",
            json!({
                "sourceAssetId": uuid(700),
                "projection": [column(101), column(102), column(103)]
            }),
        )
    }

    fn output_config() -> NodeConfig {
        config(
            5,
            "stillflow.node.output",
            json!({"outputLabel": "cleaned"}),
        )
    }

    #[test]
    fn source_project_trim_fill_and_output_lower_one_node_each() {
        let nodes = vec![
            source_config(),
            config(
                2,
                "stillflow.node.select",
                json!({"columns": [column(101), column(102)]}),
            ),
            config(3, "stillflow.node.trim", json!({"column": column(101)})),
            config(
                4,
                "stillflow.node.fill-null",
                json!({"column": column(102), "value": serialized(&ScalarValue::Int64(0))}),
            ),
            output_config(),
        ];
        let compiled = NodeGraphCompiler::default()
            .compile(
                &graph(nodes, vec![edge(1, 2), edge(2, 3), edge(3, 4), edge(4, 5)]),
                &source(),
                CompileTarget::Execution,
            )
            .expect("compile");
        assert_eq!(compiled.plan.nodes.len(), 5);
        assert_eq!(compiled.node_schemas.len(), 5);
        assert_eq!(compiled.plan.root, PlanNodeId::from_uuid(uuid(5)));
        assert!(
            !compiled.node_schemas[&node(4)]
                .field(column(102))
                .unwrap()
                .nullable
        );
        assert_eq!(
            compiled
                .plan
                .nodes
                .values()
                .filter(|node| matches!(node.kind, PlanNodeKind::Materialize { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn same_graph_shape_is_byte_and_fingerprint_deterministic_and_preview_maps_identity() {
        let nodes = vec![
            source_config(),
            config(
                2,
                "stillflow.node.cast",
                json!({
                    "column": column(102),
                    "dataType": serialized(&LogicalType::Float64),
                    "onFailure": "error"
                }),
            ),
            config(
                3,
                "stillflow.node.filter",
                json!({
                    "predicate": serialized(&Expr::Binary {
                        left: Box::new(Expr::Column(column(102))),
                        operator: BinaryOperator::GreaterThan,
                        right: Box::new(Expr::Literal(ScalarValue::Int64(0)))
                    })
                }),
            ),
            output_config(),
        ];
        let edges = vec![edge(1, 2), edge(2, 3), edge(3, 5)];
        let first = NodeGraphCompiler::default()
            .compile(
                &graph(nodes.clone(), edges.clone()),
                &source(),
                CompileTarget::Execution,
            )
            .expect("first compile");
        let second = NodeGraphCompiler::default()
            .compile(
                &graph(
                    nodes,
                    vec![edges[2].clone(), edges[0].clone(), edges[1].clone()],
                ),
                &source(),
                CompileTarget::Preview(node(3)),
            )
            .expect("second compile");
        assert_eq!(
            first.canonical_bytes().unwrap(),
            second.canonical_bytes().unwrap()
        );
        assert_eq!(first.fingerprint().unwrap(), second.fingerprint().unwrap());
        assert_eq!(
            second.preview_plan_node_id(node(3)).unwrap(),
            PlanNodeId::from_uuid(uuid(3))
        );
        assert!(second.preview_plan_node_id(node(5)).is_err());
    }

    #[test]
    fn rename_derive_drop_propagates_identity_and_rejects_removed_reference() {
        let nodes = vec![
            source_config(),
            config(
                2,
                "stillflow.node.rename",
                json!({"column": column(101), "to": "label"}),
            ),
            config(
                3,
                "stillflow.node.derive-column",
                json!({
                    "id": column(104),
                    "name": "age_copy",
                    "dataType": serialized(&LogicalType::Int64),
                    "nullable": true,
                    "expression": serialized(&Expr::Column(column(102)))
                }),
            ),
            config(
                4,
                "stillflow.node.drop-column",
                json!({"column": column(101)}),
            ),
            output_config(),
        ];
        let compiled = NodeGraphCompiler::default()
            .compile(
                &graph(nodes, vec![edge(1, 2), edge(2, 3), edge(3, 4), edge(4, 5)]),
                &source(),
                CompileTarget::Execution,
            )
            .expect("compile");
        assert_eq!(compiled.node_schemas[&node(2)].fields[0].name, "label");
        assert!(compiled.node_schemas[&node(3)].field(column(104)).is_some());
        assert!(compiled.node_schemas[&node(4)].field(column(101)).is_none());

        let invalid = vec![
            source_config(),
            config(
                2,
                "stillflow.node.drop-column",
                json!({"column": column(101)}),
            ),
            config(
                3,
                "stillflow.node.derive-column",
                json!({
                    "id": column(104),
                    "name": "bad",
                    "dataType": serialized(&LogicalType::Utf8),
                    "nullable": true,
                    "expression": serialized(&Expr::Column(column(101)))
                }),
            ),
            output_config(),
        ];
        let error = NodeGraphCompiler::default()
            .compile(
                &graph(invalid, vec![edge(1, 2), edge(2, 3), edge(3, 5)]),
                &source(),
                CompileTarget::Execution,
            )
            .expect_err("removed column must fail");
        assert_eq!(error.code(), NodeGraphErrorCode::UnknownColumn);
    }

    #[test]
    fn source_binding_and_invalid_topology_fail_closed() {
        let nodes = vec![source_config(), output_config()];
        let error = NodeGraphCompiler::default()
            .compile(
                &graph(nodes.clone(), vec![edge(1, 5)]),
                &AuthorizedSourceContext::new(uuid(701), source_schema()).unwrap(),
                CompileTarget::Execution,
            )
            .expect_err("source binding");
        assert_eq!(error.code(), NodeGraphErrorCode::SourceBinding);

        let branching = NodeGraph::new(
            uuid(901),
            node(1),
            node(5),
            vec![
                source_config(),
                output_config(),
                config(
                    6,
                    "stillflow.node.select",
                    json!({"columns": [column(101)]}),
                ),
            ],
            vec![edge(1, 5), edge(1, 6)],
            BTreeMap::new(),
        )
        .expect("basic graph shape");
        let error = NodeGraphCompiler::default()
            .compile(&branching, &source(), CompileTarget::Execution)
            .expect_err("compiler rejects branching");
        assert_eq!(error.code(), NodeGraphErrorCode::InvalidTopology);
    }
}
