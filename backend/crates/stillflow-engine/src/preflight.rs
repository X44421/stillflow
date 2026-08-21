use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use stillflow_connectors::{Capability, ConnectorRegistry};
use stillflow_core::{
    ensure_no_secret_fields, ColumnId, ConnectorKind, Expr, InspectRequest, LogicalField,
    LogicalSchema, LogicalType, RequestContext, ScalarValue, SourceAsset, SourceConnection,
};
use stillflow_plan::{CastFailurePolicy, LogicalPlan, PlanNodeId, PlanNodeKind, Rule};

use crate::error::{deadline_too_long, map_context_error, EngineError};
use crate::{
    ENGINE_MAX_DEADLINE, MAX_COMPILED_PLAN_BYTES, MAX_DEDUP_KEY_COLUMNS, MAX_EXPR_DEPTH,
    MAX_EXPR_NODES, MAX_PLAN_NODES, MAX_RULES_PER_NODE, MAX_VALIDATION_MESSAGE_BYTES,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreflightMode {
    Materialize,
    Verification,
}

#[derive(Debug, Clone)]
pub(crate) enum CompiledStep {
    Project {
        columns: Vec<ColumnId>,
    },
    Filter {
        predicate: Expr,
    },
    Rules {
        node_id: PlanNodeId,
        rules: Vec<Rule>,
    },
}

#[derive(Debug, Clone)]
pub struct PreparedPlan {
    pub(crate) push_projection: bool,
    pub(crate) scan_projection: Vec<ColumnId>,
    pub(crate) expected_connector: LogicalSchema,
    pub(crate) scan_output: LogicalSchema,
    pub(crate) materialize_schema: LogicalSchema,
    pub(crate) steps: Vec<CompiledStep>,
    pub(crate) scan_step_count: usize,
    #[allow(dead_code)]
    pub(crate) materialize_id: PlanNodeId,
}

pub(crate) async fn preflight(
    registry: &ConnectorRegistry,
    plan: &LogicalPlan,
    connection: &SourceConnection,
    asset: &SourceAsset,
    schema_override: Option<&LogicalSchema>,
    context: &RequestContext,
    mode: PreflightMode,
) -> Result<PreparedPlan, EngineError> {
    context.ensure_active().map_err(map_context_error)?;
    if context
        .remaining()
        .is_some_and(|remaining| remaining > ENGINE_MAX_DEADLINE)
    {
        return Err(deadline_too_long(Duration::from_secs(0)));
    }

    validate_plan_exprs_iterative(plan)?;
    plan.validate()
        .map_err(|_| EngineError::InvalidPlan("logical plan failed validation"))?;
    if compiled_plan_bytes(plan) > MAX_COMPILED_PLAN_BYTES {
        return Err(EngineError::BoundExceeded(
            "compiled plan exceeds MAX_COMPILED_PLAN_BYTES",
        ));
    }
    for (node_id, node) in &plan.nodes {
        if matches!(node.kind, PlanNodeKind::Join { .. } | PlanNodeKind::Union) {
            return Err(EngineError::unsupported_operator(*node_id, &node.kind));
        }
    }
    reject_paused_plan_exprs(plan)?;
    connection
        .validate()
        .map_err(|_| EngineError::SourceBinding)?;

    if plan.nodes.is_empty() || plan.nodes.len() > MAX_PLAN_NODES {
        return Err(EngineError::BoundExceeded(
            "plan node count is outside the authorized range",
        ));
    }

    let linear = linearize(plan)?;
    let (scan_id, scan_projection, scan_predicate) = match &linear[0].1.kind {
        PlanNodeKind::Scan {
            source_asset_id,
            projection,
            predicate,
        } => {
            bind_scan(*source_asset_id, connection, asset)?;
            (linear[0].0, projection.clone(), predicate.clone())
        }
        other => return Err(EngineError::unsupported_operator(linear[0].0, other)),
    };

    let materialize_id = linear[linear.len() - 1].0;
    match &linear[linear.len() - 1].1.kind {
        PlanNodeKind::Materialize { output_label } => {
            validate_output_label(output_label)?;
        }
        other => return Err(EngineError::unsupported_operator(materialize_id, other)),
    }

    reject_phase_kinds(connection.kind())?;
    let capabilities = registry
        .capabilities(connection.kind())
        .map_err(EngineError::from_connector)?;
    capabilities
        .ensure(Capability::Streaming)
        .map_err(EngineError::from_connector)?;
    let push_projection = capabilities.supports(Capability::ColumnProjection);

    let mut steps = Vec::new();
    if !push_projection {
        steps.push(CompiledStep::Project {
            columns: scan_projection.clone(),
        });
    }
    if let Some(predicate) = scan_predicate {
        steps.push(CompiledStep::Filter { predicate });
    }
    let scan_step_count = steps.len();

    for (node_id, node) in linear.iter().skip(1).take(linear.len().saturating_sub(2)) {
        match &node.kind {
            PlanNodeKind::Project { columns } => steps.push(CompiledStep::Project {
                columns: columns.clone(),
            }),
            PlanNodeKind::Filter { predicate } => steps.push(CompiledStep::Filter {
                predicate: predicate.clone(),
            }),
            PlanNodeKind::ApplyRules { rules } => {
                if rules.is_empty() || rules.len() > MAX_RULES_PER_NODE {
                    return Err(EngineError::BoundExceeded(
                        "apply-rules count is outside the authorized range",
                    ));
                }
                for rule in rules {
                    match rule {
                        Rule::Validate { .. } if mode == PreflightMode::Materialize => {
                            return Err(EngineError::UnsupportedRule {
                                node: node_id.as_uuid(),
                                kind: "validate",
                            });
                        }
                        Rule::Deduplicate { .. } if mode == PreflightMode::Materialize => {
                            return Err(EngineError::UnsupportedRule {
                                node: node_id.as_uuid(),
                                kind: "deduplicate",
                            });
                        }
                        _ => {}
                    }
                }
                steps.push(CompiledStep::Rules {
                    node_id: *node_id,
                    rules: rules.clone(),
                });
            }
            PlanNodeKind::Join { .. } | PlanNodeKind::Union => {
                return Err(EngineError::unsupported_operator(*node_id, &node.kind));
            }
            PlanNodeKind::Scan { .. } | PlanNodeKind::Materialize { .. } => {
                return Err(EngineError::InvalidPlan("duplicate scan or materialize"));
            }
        }
    }

    let _ = scan_id;
    let authorized =
        authorized_source_schema(registry, connection, asset, schema_override, context).await?;
    reject_paused_schema(&authorized)?;
    for id in &scan_projection {
        if authorized.field(*id).is_none() {
            return Err(EngineError::UnknownColumn(*id));
        }
    }

    let expected_connector = if push_projection {
        project_schema(&authorized, &scan_projection)?
    } else {
        authorized.clone()
    };
    let scan_output = project_schema(&authorized, &scan_projection)?;
    reject_paused_schema(&scan_output)?;
    let materialize_schema = propagate_schema(&scan_output, &steps)?;
    reject_paused_schema(&materialize_schema)?;

    Ok(PreparedPlan {
        push_projection,
        scan_projection,
        expected_connector,
        scan_output,
        materialize_schema,
        steps,
        scan_step_count,
        materialize_id,
    })
}

fn linearize(
    plan: &LogicalPlan,
) -> Result<Vec<(PlanNodeId, stillflow_plan::PlanNode)>, EngineError> {
    let mut scans = Vec::new();
    let mut materializes = Vec::new();
    let mut children: BTreeMap<PlanNodeId, Vec<PlanNodeId>> = BTreeMap::new();
    for (id, node) in &plan.nodes {
        match &node.kind {
            PlanNodeKind::Scan { .. } => scans.push(*id),
            PlanNodeKind::Materialize { .. } => materializes.push(*id),
            _ => {}
        }
        if matches!(node.kind, PlanNodeKind::Scan { .. }) {
            if !node.inputs.is_empty() {
                return Err(EngineError::InvalidPlan("scan must not have inputs"));
            }
        } else if node.inputs.len() != 1 {
            return Err(EngineError::unsupported_operator(*id, &node.kind));
        } else if let Some(parent) = node.inputs.first() {
            children.entry(*parent).or_default().push(*id);
        }
    }
    if scans.len() != 1 || materializes.len() != 1 {
        return Err(EngineError::InvalidPlan(
            "plan must contain exactly one scan and one materialize",
        ));
    }
    if plan.root != materializes[0] {
        return Err(EngineError::InvalidPlan(
            "root must be the materialize node",
        ));
    }

    let mut path = Vec::new();
    let mut current = scans[0];
    let mut visited = BTreeSet::new();
    loop {
        if !visited.insert(current) {
            return Err(EngineError::InvalidPlan("plan path is cyclic"));
        }
        let node = plan
            .nodes
            .get(&current)
            .ok_or(EngineError::InvalidPlan("missing plan node"))?;
        path.push((current, node.clone()));
        if current == plan.root {
            break;
        }
        let Some(next) = children.get(&current) else {
            return Err(EngineError::InvalidPlan("disconnected plan node"));
        };
        if next.len() != 1 {
            return Err(EngineError::InvalidPlan("plan path is not linear"));
        }
        current = next[0];
    }
    if path.len() != plan.nodes.len() {
        return Err(EngineError::InvalidPlan("disconnected plan node"));
    }
    Ok(path)
}

fn bind_scan(
    source_asset_id: uuid::Uuid,
    connection: &SourceConnection,
    asset: &SourceAsset,
) -> Result<(), EngineError> {
    if source_asset_id.is_nil() || asset.id.is_nil() || connection.id().is_nil() {
        return Err(EngineError::SourceBinding);
    }
    if source_asset_id != asset.id {
        return Err(EngineError::SourceBinding);
    }
    if asset.connection_id != connection.id() {
        return Err(EngineError::SourceBinding);
    }
    Ok(())
}

fn reject_phase_kinds(kind: ConnectorKind) -> Result<(), EngineError> {
    match kind {
        ConnectorKind::SqlDatabase => Err(EngineError::UnsupportedCapability {
            kind: "sqlDatabase",
        }),
        ConnectorKind::DocumentWorker => Err(EngineError::UnsupportedCapability {
            kind: "documentWorker",
        }),
        ConnectorKind::LocalFile | ConnectorKind::ExcelWorkbook | ConnectorKind::ObjectStore => {
            Ok(())
        }
    }
}

fn validate_output_label(label: &str) -> Result<(), EngineError> {
    if label.trim().is_empty() {
        return Err(EngineError::InvalidPlan("materialize label is empty"));
    }
    Expr::Literal(ScalarValue::Utf8(label.to_owned()))
        .validate_shape()
        .map_err(|_| EngineError::InvalidPlan("materialize label is not secret-safe"))?;
    Ok(())
}

fn reject_paused_schema(schema: &LogicalSchema) -> Result<(), EngineError> {
    for field in &schema.fields {
        crate::typing::reject_paused_type(&field.data_type)?;
    }
    Ok(())
}

fn reject_paused_plan_exprs(plan: &LogicalPlan) -> Result<(), EngineError> {
    for node in plan.nodes.values() {
        match &node.kind {
            PlanNodeKind::Scan {
                predicate: Some(predicate),
                ..
            } => crate::typing::reject_paused_expr(predicate)?,
            PlanNodeKind::Scan { .. } => {}
            PlanNodeKind::Filter { predicate } => {
                crate::typing::reject_paused_expr(predicate)?;
            }
            PlanNodeKind::ApplyRules { rules } => {
                for rule in rules {
                    reject_paused_rule_exprs(rule)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn reject_paused_rule_exprs(rule: &Rule) -> Result<(), EngineError> {
    match rule {
        Rule::DeriveColumn {
            expression,
            data_type,
            ..
        } => {
            crate::typing::reject_paused_type(data_type)?;
            crate::typing::reject_paused_expr(expression)
        }
        Rule::FilterRows { predicate } => crate::typing::reject_paused_expr(predicate),
        Rule::Cast { data_type, .. } => crate::typing::reject_paused_type(data_type),
        _ => Ok(()),
    }
}

fn compiled_plan_bytes(plan: &LogicalPlan) -> usize {
    let mut bytes = plan.nodes.len().saturating_mul(64);
    for node in plan.nodes.values() {
        bytes = bytes.saturating_add(match &node.kind {
            PlanNodeKind::Materialize { output_label } => output_label.len(),
            PlanNodeKind::Filter { predicate } => expr_bytes(predicate),
            PlanNodeKind::Scan { predicate, .. } => predicate.as_ref().map_or(0, expr_bytes),
            PlanNodeKind::ApplyRules { rules } => rules.iter().map(rule_bytes).sum(),
            PlanNodeKind::Project { columns } => columns.len().saturating_mul(16),
            _ => 0,
        });
    }
    bytes
}

fn rule_bytes(rule: &Rule) -> usize {
    match rule {
        Rule::Rename { to, .. } => to.len(),
        Rule::DeriveColumn {
            name, expression, ..
        } => name.len().saturating_add(expr_bytes(expression)),
        Rule::ReplaceLiteral { from, to, .. } => {
            literal_bytes(from).saturating_add(literal_bytes(to))
        }
        Rule::FillNull { value, .. } => literal_bytes(value),
        Rule::FilterRows { predicate } => expr_bytes(predicate),
        Rule::Validate {
            predicate, message, ..
        } => expr_bytes(predicate).saturating_add(message.len()),
        Rule::Trim { .. }
        | Rule::DropColumn { .. }
        | Rule::Cast { .. }
        | Rule::Deduplicate { .. } => 0,
    }
}

fn expr_bytes(expr: &Expr) -> usize {
    match expr {
        Expr::Literal(value) => literal_bytes(value),
        Expr::Unary { expression, .. }
        | Expr::IsNull { expression, .. }
        | Expr::Cast { expression, .. } => expr_bytes(expression),
        Expr::Binary { left, right, .. } => expr_bytes(left).saturating_add(expr_bytes(right)),
        Expr::Coalesce { expressions } => expressions.iter().map(expr_bytes).sum(),
        Expr::Column(_) => 16,
    }
}

fn literal_bytes(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::Utf8(text) => text.len(),
        _ => 8,
    }
}

async fn authorized_source_schema(
    registry: &ConnectorRegistry,
    connection: &SourceConnection,
    asset: &SourceAsset,
    schema_override: Option<&LogicalSchema>,
    context: &RequestContext,
) -> Result<LogicalSchema, EngineError> {
    if let Some(schema) = schema_override {
        schema
            .validate()
            .map_err(|_| EngineError::InvalidPlan("schema override is invalid"))?;
        return Ok(schema.clone());
    }
    context.ensure_active().map_err(map_context_error)?;
    let metadata = registry
        .inspect(
            connection,
            InspectRequest {
                context: context.clone(),
                asset: asset.clone(),
            },
        )
        .await
        .map_err(EngineError::from_connector)?;
    context.ensure_active().map_err(map_context_error)?;
    metadata
        .schema
        .validate()
        .map_err(|_| EngineError::InvalidPlan("inspected schema is invalid"))?;
    Ok(metadata.schema)
}

pub(crate) fn project_schema(
    schema: &LogicalSchema,
    columns: &[ColumnId],
) -> Result<LogicalSchema, EngineError> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut seen = BTreeSet::new();
    for id in columns {
        if !seen.insert(*id) {
            return Err(EngineError::InvalidPlan(
                "projection contains duplicate columns",
            ));
        }
        let field = schema
            .field(*id)
            .ok_or(EngineError::UnknownColumn(*id))?
            .clone();
        fields.push(field);
    }
    LogicalSchema::new(fields).map_err(|_| EngineError::InvalidPlan("projected schema is invalid"))
}

fn propagate_schema(
    scan_output: &LogicalSchema,
    steps: &[CompiledStep],
) -> Result<LogicalSchema, EngineError> {
    let mut schema = scan_output.clone();
    for step in steps {
        match step {
            CompiledStep::Project { columns } => schema = project_schema(&schema, columns)?,
            CompiledStep::Filter { predicate } => {
                crate::typing::require_boolean(predicate, &schema)?;
            }
            CompiledStep::Rules { rules, .. } => {
                for rule in rules {
                    schema = apply_rule_schema(schema, rule)?;
                }
            }
        }
    }
    Ok(schema)
}

fn apply_rule_schema(mut schema: LogicalSchema, rule: &Rule) -> Result<LogicalSchema, EngineError> {
    match rule {
        Rule::Rename { column, to } => {
            schema
                .rename_column(*column, to.clone())
                .map_err(|_| EngineError::UnknownColumn(*column))?;
            Ok(schema)
        }
        Rule::DropColumn { column } => {
            if schema.fields.len() <= 1 {
                return Err(EngineError::InvalidPlan(
                    "cannot drop the last remaining column",
                ));
            }
            let fields: Vec<LogicalField> = schema
                .fields
                .iter()
                .filter(|field| field.id != *column)
                .cloned()
                .collect();
            if fields.len() == schema.fields.len() {
                return Err(EngineError::UnknownColumn(*column));
            }
            LogicalSchema::new(fields)
                .map_err(|_| EngineError::InvalidPlan("drop produced an invalid schema"))
        }
        Rule::Trim { column } => {
            let field = schema
                .field(*column)
                .ok_or(EngineError::UnknownColumn(*column))?;
            if !matches!(field.data_type, LogicalType::Utf8) {
                return Err(EngineError::TypeError("trim requires a utf8 column"));
            }
            Ok(schema)
        }
        Rule::Cast {
            column,
            data_type,
            on_failure,
        } => {
            crate::typing::reject_paused_type(data_type)?;
            reject_paused_cast(
                &schema
                    .field(*column)
                    .ok_or(EngineError::UnknownColumn(*column))?
                    .data_type,
                data_type,
            )?;
            let mut fields = schema.fields.clone();
            let field = fields
                .iter_mut()
                .find(|field| field.id == *column)
                .ok_or(EngineError::UnknownColumn(*column))?;
            field.data_type = data_type.clone();
            if matches!(on_failure, CastFailurePolicy::SetNull) {
                field.nullable = true;
            }
            LogicalSchema::new(fields)
                .map_err(|_| EngineError::InvalidPlan("cast produced an invalid schema"))
        }
        Rule::ReplaceLiteral { column, from, to } => {
            let field = schema
                .field(*column)
                .ok_or(EngineError::UnknownColumn(*column))?;
            validate_literal_for_column(&field.data_type, from)?;
            validate_literal_for_column(&field.data_type, to)?;
            if matches!(field.data_type, LogicalType::Binary)
                && !matches!((from, to), (ScalarValue::Null, ScalarValue::Null))
            {
                return Err(EngineError::TypeError(
                    "binary replace-literal may only use null-to-null",
                ));
            }
            if matches!(to, ScalarValue::Null) {
                let mut fields = schema.fields.clone();
                if let Some(field) = fields.iter_mut().find(|field| field.id == *column) {
                    field.nullable = true;
                }
                return LogicalSchema::new(fields)
                    .map_err(|_| EngineError::InvalidPlan("replace produced an invalid schema"));
            }
            Ok(schema)
        }
        Rule::FillNull { column, value } => {
            if matches!(value, ScalarValue::Null) {
                return Err(EngineError::TypeError("fill-null value must not be null"));
            }
            let field = schema
                .field(*column)
                .ok_or(EngineError::UnknownColumn(*column))?;
            if matches!(field.data_type, LogicalType::Binary) {
                return Err(EngineError::TypeError(
                    "fill-null is not authorized on binary",
                ));
            }
            validate_literal_for_column(&field.data_type, value)?;
            let mut fields = schema.fields.clone();
            if let Some(field) = fields.iter_mut().find(|field| field.id == *column) {
                field.nullable = false;
            }
            LogicalSchema::new(fields)
                .map_err(|_| EngineError::InvalidPlan("fill-null produced an invalid schema"))
        }
        Rule::DeriveColumn {
            id,
            name,
            data_type,
            nullable,
            expression,
        } => {
            validate_expr(expression, &schema)?;
            let inferred = crate::typing::type_check_expr(expression, &schema)?;
            crate::typing::reject_paused_type(data_type)?;
            if !matches!(inferred, LogicalType::Null) && inferred != *data_type {
                return Err(EngineError::TypeError(
                    "derived column type does not match the typed expression",
                ));
            }
            if schema.field(*id).is_some() || schema.fields.iter().any(|field| field.name == *name)
            {
                return Err(EngineError::InvalidPlan(
                    "derived column id or name is not unique",
                ));
            }
            reject_paused_cast_in_expr(expression, &schema)?;
            let nullable_inferred = infer_nullability(expression, &schema)?;
            if !*nullable && nullable_inferred {
                return Err(EngineError::TypeError(
                    "derived column nullability is narrower than the expression",
                ));
            }
            let mut fields = schema.fields.clone();
            fields.push(
                LogicalField::new(*id, name.clone(), data_type.clone(), *nullable)
                    .map_err(|_| EngineError::InvalidPlan("derived field is invalid"))?,
            );
            LogicalSchema::new(fields)
                .map_err(|_| EngineError::InvalidPlan("derive produced an invalid schema"))
        }
        Rule::FilterRows { predicate } => {
            crate::typing::require_boolean(predicate, &schema)?;
            Ok(schema)
        }
        Rule::Validate {
            predicate, message, ..
        } => {
            crate::typing::require_boolean(predicate, &schema)?;
            let trimmed = message.trim();
            if trimmed.is_empty() {
                return Err(EngineError::InvalidPlan(
                    "validation message must not be empty",
                ));
            }
            if trimmed.len() > MAX_VALIDATION_MESSAGE_BYTES {
                return Err(EngineError::BoundExceeded(
                    "validation message exceeds MAX_VALIDATION_MESSAGE_BYTES",
                ));
            }
            ensure_no_secret_fields(&serde_json::Value::String(message.clone()))
                .map_err(|_| EngineError::InvalidPlan("validation message is not secret-safe"))?;
            Expr::Literal(ScalarValue::Utf8(message.clone()))
                .validate_shape()
                .map_err(|_| {
                    EngineError::InvalidPlan("validation message failed shape validation")
                })?;
            Ok(schema)
        }
        Rule::Deduplicate { keys } => {
            if keys.len() > MAX_DEDUP_KEY_COLUMNS {
                return Err(EngineError::BoundExceeded(
                    "dedup key count exceeds MAX_DEDUP_KEY_COLUMNS",
                ));
            }
            for key in keys {
                let field = schema.field(*key).ok_or(EngineError::UnknownColumn(*key))?;
                crate::typing::reject_paused_type(&field.data_type)?;
                if matches!(
                    field.data_type,
                    LogicalType::Timestamp {
                        unit: stillflow_core::TimeUnit::Second,
                        ..
                    }
                ) {
                    return Err(EngineError::TypeError(
                        "timestamp second unit is paused for dedup keys",
                    ));
                }
            }
            Ok(schema)
        }
    }
}

fn reject_paused_cast(from: &LogicalType, to: &LogicalType) -> Result<(), EngineError> {
    if matches!(from, LogicalType::Date32 | LogicalType::Timestamp { .. })
        && matches!(to, LogicalType::Utf8)
    {
        return Err(EngineError::TypeError(
            "cast from date32 or timestamp to utf8 is paused",
        ));
    }
    if (matches!(to, LogicalType::Binary) && !matches!(from, LogicalType::Binary))
        || (matches!(from, LogicalType::Binary) && !matches!(to, LogicalType::Binary))
    {
        return Err(EngineError::TypeError(
            "cast to/from binary is not authorized",
        ));
    }
    Ok(())
}

fn reject_paused_cast_in_expr(expr: &Expr, schema: &LogicalSchema) -> Result<(), EngineError> {
    match expr {
        Expr::Cast {
            expression,
            data_type,
        } => {
            let from = crate::typing::type_check_expr(expression, schema)?;
            reject_paused_cast(&from, data_type)?;
            reject_paused_cast_in_expr(expression, schema)
        }
        Expr::Unary { expression, .. } | Expr::IsNull { expression, .. } => {
            reject_paused_cast_in_expr(expression, schema)
        }
        Expr::Binary { left, right, .. } => {
            reject_paused_cast_in_expr(left, schema)?;
            reject_paused_cast_in_expr(right, schema)
        }
        Expr::Coalesce { expressions } => {
            for expr in expressions {
                reject_paused_cast_in_expr(expr, schema)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_literal_for_column(
    column_type: &LogicalType,
    value: &ScalarValue,
) -> Result<(), EngineError> {
    match (column_type, value) {
        (_, ScalarValue::Null) => Ok(()),
        (LogicalType::Boolean, ScalarValue::Boolean(_))
        | (LogicalType::Int64, ScalarValue::Int64(_))
        | (LogicalType::UInt64, ScalarValue::UInt64(_))
        | (LogicalType::Float64, ScalarValue::Float64(_))
        | (LogicalType::Utf8, ScalarValue::Utf8(_)) => Ok(()),
        (LogicalType::Int8 | LogicalType::Int16 | LogicalType::Int32, ScalarValue::Int64(_))
        | (
            LogicalType::UInt8 | LogicalType::UInt16 | LogicalType::UInt32,
            ScalarValue::UInt64(_),
        )
        | (LogicalType::Float32, ScalarValue::Float64(_)) => Ok(()),
        _ => Err(EngineError::TypeError(
            "literal is not type-compatible with the column",
        )),
    }
}

fn validate_expr_iterative(expr: &Expr) -> Result<(), EngineError> {
    let mut nodes = 0_usize;
    let mut max_depth = 0_usize;
    let mut stack = vec![(expr, 1_usize)];
    while let Some((current, depth)) = stack.pop() {
        nodes += 1;
        max_depth = max_depth.max(depth);
        if nodes > MAX_EXPR_NODES || max_depth > MAX_EXPR_DEPTH {
            return Err(EngineError::BoundExceeded(
                "expression exceeds node or depth limits",
            ));
        }
        match current {
            Expr::Column(_) => {}
            Expr::Literal(ScalarValue::Utf8(value)) => {
                Expr::Literal(ScalarValue::Utf8(value.clone()))
                    .validate_shape()
                    .map_err(|_| {
                        EngineError::InvalidPlan("literal contains prohibited secret pattern")
                    })?;
            }
            Expr::Literal(ScalarValue::Float64(value)) => {
                if !value.get().is_finite() {
                    return Err(EngineError::InvalidPlan("float literal is not finite"));
                }
            }
            Expr::Literal(_) => {}
            Expr::Unary { expression, .. } | Expr::IsNull { expression, .. } => {
                stack.push((expression, depth + 1));
            }
            Expr::Cast {
                expression,
                data_type,
            } => {
                data_type
                    .validate()
                    .map_err(|_| EngineError::InvalidPlan("cast target data type is invalid"))?;
                stack.push((expression, depth + 1));
            }
            Expr::Binary { left, right, .. } => {
                stack.push((left, depth + 1));
                stack.push((right, depth + 1));
            }
            Expr::Coalesce { expressions } => {
                if expressions.is_empty() {
                    return Err(EngineError::InvalidPlan(
                        "coalesce expression list is empty",
                    ));
                }
                for expr in expressions {
                    stack.push((expr, depth + 1));
                }
            }
        }
    }
    Ok(())
}

fn validate_plan_exprs_iterative(plan: &LogicalPlan) -> Result<(), EngineError> {
    for node in plan.nodes.values() {
        match &node.kind {
            PlanNodeKind::Scan {
                predicate: Some(predicate),
                ..
            }
            | PlanNodeKind::Filter { predicate } => {
                validate_expr_iterative(predicate)?;
            }
            PlanNodeKind::ApplyRules { rules } => {
                for rule in rules {
                    match rule {
                        Rule::DeriveColumn { expression, .. } => {
                            validate_expr_iterative(expression)?;
                        }
                        Rule::FilterRows { predicate } => {
                            validate_expr_iterative(predicate)?;
                        }
                        Rule::Validate { predicate, .. } => {
                            validate_expr_iterative(predicate)?;
                        }
                        _ => {}
                    }
                }
            }
            PlanNodeKind::Join { keys, .. } => {
                for key in keys {
                    validate_expr_iterative(&key.left)?;
                    validate_expr_iterative(&key.right)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_expr(expr: &Expr, schema: &LogicalSchema) -> Result<(), EngineError> {
    expr.validate_shape()
        .map_err(|_| EngineError::InvalidPlan("expression failed shape validation"))?;
    let mut nodes = 0_usize;
    let mut max_depth = 0_usize;
    let mut stack = vec![(expr, 1_usize)];
    while let Some((current, depth)) = stack.pop() {
        nodes += 1;
        max_depth = max_depth.max(depth);
        if nodes > MAX_EXPR_NODES || max_depth > MAX_EXPR_DEPTH {
            return Err(EngineError::BoundExceeded(
                "expression exceeds node or depth limits",
            ));
        }
        match current {
            Expr::Column(id) => {
                if schema.field(*id).is_none() {
                    return Err(EngineError::UnknownColumn(*id));
                }
            }
            Expr::Unary { expression, .. }
            | Expr::IsNull { expression, .. }
            | Expr::Cast { expression, .. } => stack.push((expression, depth + 1)),
            Expr::Binary { left, right, .. } => {
                stack.push((left, depth + 1));
                stack.push((right, depth + 1));
            }
            Expr::Coalesce { expressions } => {
                for expr in expressions {
                    stack.push((expr, depth + 1));
                }
            }
            Expr::Literal(_) => {}
        }
    }
    Ok(())
}

fn infer_nullability(expr: &Expr, schema: &LogicalSchema) -> Result<bool, EngineError> {
    Ok(match expr {
        Expr::Column(id) => {
            schema
                .field(*id)
                .ok_or(EngineError::UnknownColumn(*id))?
                .nullable
        }
        Expr::Literal(ScalarValue::Null) => true,
        Expr::Literal(_) => false,
        Expr::Unary { expression, .. } | Expr::Cast { expression, .. } => {
            infer_nullability(expression, schema)?
        }
        Expr::Binary { left, right, .. } => {
            infer_nullability(left, schema)? || infer_nullability(right, schema)?
        }
        Expr::IsNull { .. } => false,
        Expr::Coalesce { expressions } => {
            let mut all_nullable = true;
            for expr in expressions {
                all_nullable &= infer_nullability(expr, schema)?;
            }
            all_nullable
        }
    })
}
