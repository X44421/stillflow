use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use stillflow_connectors::{Capability, ConnectorRegistry};
use stillflow_core::{
    ColumnId, ConnectorKind, Expr, InspectRequest, LogicalField, LogicalSchema, LogicalType,
    RequestContext, ScalarValue, SourceAsset, SourceConnection,
};
use stillflow_plan::{CastFailurePolicy, LogicalPlan, PlanNodeId, PlanNodeKind, Rule};

use crate::error::{deadline_too_long, map_context_error, EngineError};
use crate::lookup::{AuthorizedLookup, ColumnLookup, WorkingSchema};
use crate::{
    ENGINE_MAX_DEADLINE, MAX_COMPILED_PLAN_BYTES, MAX_EXPR_DEPTH, MAX_EXPR_NODES, MAX_PLAN_NODES,
    MAX_RULES_PER_NODE,
};

#[derive(Debug, Clone)]
pub(crate) enum CompiledStep {
    Project { columns: Vec<ColumnId> },
    Filter { predicate: Expr },
    Rules { rules: Vec<Rule> },
}

#[derive(Debug, Clone)]
pub struct PreparedPlan {
    pub(crate) push_projection: bool,
    pub(crate) scan_projection: Vec<ColumnId>,
    pub(crate) expected_connector: LogicalSchema,
    pub(crate) scan_output: LogicalSchema,
    pub(crate) materialize_schema: LogicalSchema,
    pub(crate) steps: Vec<CompiledStep>,
    pub(crate) target_steps: Vec<CompiledStep>,
    pub(crate) target_schema: LogicalSchema,
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
    preview_target: Option<PlanNodeId>,
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
    let target_index = match preview_target {
        Some(target) => {
            let Some(index) = linear.iter().position(|(id, _)| *id == target) else {
                return Err(EngineError::InvalidPlan(
                    "preview target is not on the linear path",
                ));
            };
            if index == linear.len() - 1 {
                return Err(EngineError::UnsupportedOperator {
                    node: target.as_uuid(),
                    kind: "materialize",
                });
            }
            match &linear[index].1.kind {
                PlanNodeKind::Scan { .. }
                | PlanNodeKind::Project { .. }
                | PlanNodeKind::Filter { .. }
                | PlanNodeKind::ApplyRules { .. } => {}
                other => return Err(EngineError::unsupported_operator(target, other)),
            }
            Some(index)
        }
        None => None,
    };

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
    let mut preview_steps = Vec::new();
    if !push_projection {
        let step = CompiledStep::Project {
            columns: scan_projection.clone(),
        };
        if preview_target.is_some() {
            preview_steps.push(step.clone());
        }
        steps.push(step);
    }
    if let Some(predicate) = scan_predicate {
        let step = CompiledStep::Filter { predicate };
        if preview_target.is_some() {
            preview_steps.push(step.clone());
        }
        steps.push(step);
    }

    for (position, (node_id, node)) in linear
        .iter()
        .enumerate()
        .skip(1)
        .take(linear.len().saturating_sub(2))
    {
        let in_preview = target_index.is_some_and(|index| position <= index);
        match &node.kind {
            PlanNodeKind::Project { columns } => {
                let step = CompiledStep::Project {
                    columns: columns.clone(),
                };
                if in_preview {
                    preview_steps.push(step.clone());
                }
                steps.push(step);
            }
            PlanNodeKind::Filter { predicate } => {
                let step = CompiledStep::Filter {
                    predicate: predicate.clone(),
                };
                if in_preview {
                    preview_steps.push(step.clone());
                }
                steps.push(step);
            }
            PlanNodeKind::ApplyRules { rules } => {
                if rules.is_empty() || rules.len() > MAX_RULES_PER_NODE {
                    return Err(EngineError::BoundExceeded(
                        "apply-rules count is outside the authorized range",
                    ));
                }
                for rule in rules {
                    match rule {
                        Rule::Validate { .. } => {
                            return Err(EngineError::UnsupportedRule {
                                node: node_id.as_uuid(),
                                kind: "validate",
                            });
                        }
                        Rule::Deduplicate { .. } => {
                            return Err(EngineError::UnsupportedRule {
                                node: node_id.as_uuid(),
                                kind: "deduplicate",
                            });
                        }
                        _ => {}
                    }
                }
                let step = CompiledStep::Rules {
                    rules: rules.clone(),
                };
                if in_preview {
                    preview_steps.push(step.clone());
                }
                steps.push(step);
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
    // One Engine-private lookup view over the exact validated schema state,
    // shared by the projection existence check and both scan projections.
    // The deterministic shape policy builds the ordinal index only when the
    // lookups this projection will serve amortize its build; otherwise the
    // linear reference resolution runs unchanged.
    let authorized_lookup = AuthorizedLookup::for_shape(
        &authorized,
        projection_served_lookups(&scan_projection, push_projection),
    );
    for id in &scan_projection {
        if authorized_lookup.lookup_field(*id).is_none() {
            return Err(EngineError::UnknownColumn(*id));
        }
    }

    let expected_connector = if push_projection {
        project_schema_with(&authorized_lookup, &scan_projection)?
    } else {
        authorized.clone()
    };
    let scan_output = project_schema_with(&authorized_lookup, &scan_projection)?;
    reject_paused_schema(&scan_output)?;
    let materialize_schema = propagate_schema(&scan_output, &steps)?;
    reject_paused_schema(&materialize_schema)?;
    let (target_steps, target_schema) = match preview_target {
        Some(_) => {
            let schema = propagate_schema(&scan_output, &preview_steps)?;
            reject_paused_schema(&schema)?;
            (preview_steps, schema)
        }
        None => (steps.clone(), materialize_schema.clone()),
    };

    Ok(PreparedPlan {
        push_projection,
        scan_projection,
        expected_connector,
        scan_output,
        materialize_schema,
        steps,
        target_steps,
        target_schema,
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

/// Exact count of `ColumnId` resolutions the authorized-level lookup will
/// serve in `preflight`: the scan-projection existence check, then the
/// `expected_connector` and `scan_output` projections (both run when the
/// connector pushes projection; only the latter otherwise). Part of the
/// deterministic shape policy: a pure function of plan/schema shape.
fn projection_served_lookups(scan_projection: &[ColumnId], push_projection: bool) -> usize {
    let factor = if push_projection { 3 } else { 2 };
    scan_projection.len().saturating_mul(factor)
}

/// Exact count of `ColumnId` resolutions the propagation working schema will
/// serve for `steps`, mirroring the passes that run in `propagate_schema`
/// (one type-check walk per predicate, four walks per derive expression, one
/// rule-argument lookup per column rule, one per projection column). Part of
/// the deterministic shape policy: a pure function of plan/schema shape.
fn steps_served_lookups(steps: &[CompiledStep]) -> usize {
    steps.iter().map(step_served_lookups).sum()
}

fn step_served_lookups(step: &CompiledStep) -> usize {
    match step {
        CompiledStep::Project { columns } => columns.len(),
        CompiledStep::Filter { predicate } => crate::typing::count_expr_column_refs(predicate),
        CompiledStep::Rules { rules } => rules.iter().map(rule_served_lookups).sum(),
    }
}

fn rule_served_lookups(rule: &Rule) -> usize {
    match rule {
        Rule::Rename { .. } => 1,
        // DropColumn filters the field list directly and performs no
        // `ColumnId` resolution through the lookup backend.
        Rule::DropColumn { .. } => 0,
        Rule::Trim { .. } => 1,
        Rule::Cast { .. } => 1,
        Rule::ReplaceLiteral { .. } => 1,
        Rule::FillNull { .. } => 1,
        Rule::DeriveColumn { expression, .. } => {
            4 * crate::typing::count_expr_column_refs(expression) + 1
        }
        Rule::FilterRows { predicate } => crate::typing::count_expr_column_refs(predicate),
        // Rejected during step compilation, before schema propagation.
        Rule::Validate { .. } | Rule::Deduplicate { .. } => 0,
    }
}

pub(crate) fn project_schema(
    schema: &LogicalSchema,
    columns: &[ColumnId],
) -> Result<LogicalSchema, EngineError> {
    // Standalone calls (e.g. lowering) decide by the same deterministic
    // policy over the lookups this single call will serve.
    project_schema_with(&AuthorizedLookup::for_shape(schema, columns.len()), columns)
}

fn project_schema_with<L: ColumnLookup + ?Sized>(
    lookup: &L,
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
        let field = lookup
            .lookup_field(*id)
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
    // One working schema threaded through the whole propagation. The
    // deterministic shape policy picks the indexed Engine-private backend
    // when the lookups this step list will serve amortize its build; the
    // linear backend is the unchanged reference behavior. Every mutation
    // rebuilds the private ordinal table from the exact new validated schema
    // state, so no stale window exists.
    let working = WorkingSchema::for_shape(scan_output.clone(), steps_served_lookups(steps));
    propagate_schema_with(scan_output, steps, working)
}

/// Propagation over an explicitly chosen working-schema backend. Production
/// calls this through [`propagate_schema`]; the differential suite drives the
/// linear reference and indexed backends over identical inputs and requires
/// byte-identical outcomes.
fn propagate_schema_with(
    _scan_output: &LogicalSchema,
    steps: &[CompiledStep],
    mut working: WorkingSchema,
) -> Result<LogicalSchema, EngineError> {
    for step in steps {
        match step {
            CompiledStep::Project { columns } => {
                let projected = project_schema_with(&working, columns)?;
                working.swap_with(projected);
            }
            CompiledStep::Filter { predicate } => {
                crate::typing::require_boolean_in(predicate, &working)?;
            }
            CompiledStep::Rules { rules } => {
                for rule in rules {
                    apply_rule_schema_in(&mut working, rule)?;
                }
            }
        }
    }
    Ok(working.into_schema())
}

fn apply_rule_schema_in(schema: &mut WorkingSchema, rule: &Rule) -> Result<(), EngineError> {
    match rule {
        Rule::Rename { column, to } => schema.rename(*column, to.clone()),
        Rule::DropColumn { column } => {
            if schema.schema().fields.len() <= 1 {
                return Err(EngineError::InvalidPlan(
                    "cannot drop the last remaining column",
                ));
            }
            let previous_len = schema.schema().fields.len();
            let fields: Vec<LogicalField> = schema
                .schema()
                .fields
                .iter()
                .filter(|field| field.id != *column)
                .cloned()
                .collect();
            if fields.len() == previous_len {
                return Err(EngineError::UnknownColumn(*column));
            }
            schema.store(fields, "drop produced an invalid schema")
        }
        Rule::Trim { column } => {
            let field = schema
                .lookup_field(*column)
                .ok_or(EngineError::UnknownColumn(*column))?;
            if !matches!(field.data_type, LogicalType::Utf8) {
                return Err(EngineError::TypeError("trim requires a utf8 column"));
            }
            Ok(())
        }
        Rule::Cast {
            column,
            data_type,
            on_failure,
        } => {
            crate::typing::reject_paused_type(data_type)?;
            reject_paused_cast(
                &schema
                    .lookup_field(*column)
                    .ok_or(EngineError::UnknownColumn(*column))?
                    .data_type,
                data_type,
            )?;
            let mut fields = schema.schema().fields.clone();
            let field = fields
                .iter_mut()
                .find(|field| field.id == *column)
                .ok_or(EngineError::UnknownColumn(*column))?;
            field.data_type = data_type.clone();
            if matches!(on_failure, CastFailurePolicy::SetNull) {
                field.nullable = true;
            }
            schema.store(fields, "cast produced an invalid schema")
        }
        Rule::ReplaceLiteral { column, from, to } => {
            let field = schema
                .lookup_field(*column)
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
                let mut fields = schema.schema().fields.clone();
                if let Some(field) = fields.iter_mut().find(|field| field.id == *column) {
                    field.nullable = true;
                }
                return schema.store(fields, "replace produced an invalid schema");
            }
            Ok(())
        }
        Rule::FillNull { column, value } => {
            if matches!(value, ScalarValue::Null) {
                return Err(EngineError::TypeError("fill-null value must not be null"));
            }
            let field = schema
                .lookup_field(*column)
                .ok_or(EngineError::UnknownColumn(*column))?;
            if matches!(field.data_type, LogicalType::Binary) {
                return Err(EngineError::TypeError(
                    "fill-null is not authorized on binary",
                ));
            }
            validate_literal_for_column(&field.data_type, value)?;
            let mut fields = schema.schema().fields.clone();
            if let Some(field) = fields.iter_mut().find(|field| field.id == *column) {
                field.nullable = false;
            }
            schema.store(fields, "fill-null produced an invalid schema")
        }
        Rule::DeriveColumn {
            id,
            name,
            data_type,
            nullable,
            expression,
        } => {
            validate_expr(expression, schema)?;
            let inferred = crate::typing::type_check_expr_in(expression, schema)?;
            crate::typing::reject_paused_type(data_type)?;
            if !matches!(inferred, LogicalType::Null) && inferred != *data_type {
                return Err(EngineError::TypeError(
                    "derived column type does not match the typed expression",
                ));
            }
            let duplicate = schema.lookup_field(*id).is_some()
                || schema.schema().fields.iter().any(|field| field.name == *name);
            if duplicate {
                return Err(EngineError::InvalidPlan(
                    "derived column id or name is not unique",
                ));
            }
            reject_paused_cast_in_expr(expression, schema)?;
            let nullable_inferred = infer_nullability(expression, schema)?;
            if !*nullable && nullable_inferred {
                return Err(EngineError::TypeError(
                    "derived column nullability is narrower than the expression",
                ));
            }
            let mut fields = schema.schema().fields.clone();
            fields.push(
                LogicalField::new(*id, name.clone(), data_type.clone(), *nullable)
                    .map_err(|_| EngineError::InvalidPlan("derived field is invalid"))?,
            );
            schema.store(fields, "derive produced an invalid schema")
        }
        Rule::FilterRows { predicate } => crate::typing::require_boolean_in(predicate, schema),
        Rule::Validate { .. } => Err(EngineError::UnsupportedRule {
            node: uuid::Uuid::nil(),
            kind: "validate",
        }),
        Rule::Deduplicate { .. } => Err(EngineError::UnsupportedRule {
            node: uuid::Uuid::nil(),
            kind: "deduplicate",
        }),
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

fn reject_paused_cast_in_expr<L: ColumnLookup + ?Sized>(
    expr: &Expr,
    schema: &L,
) -> Result<(), EngineError> {
    match expr {
        Expr::Cast {
            expression,
            data_type,
        } => {
            let from = crate::typing::type_check_expr_in(expression, schema)?;
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

fn validate_expr<L: ColumnLookup + ?Sized>(
    expr: &Expr,
    schema: &L,
) -> Result<(), EngineError> {
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
                if schema.lookup_field(*id).is_none() {
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

fn infer_nullability<L: ColumnLookup + ?Sized>(
    expr: &Expr,
    schema: &L,
) -> Result<bool, EngineError> {
    Ok(match expr {
        Expr::Column(id) => {
            schema
                .lookup_field(*id)
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

#[cfg(test)]
mod differential_tests {
    //! Deterministic differential oracle for the productionized lookup index
    //! (O0-B2-A1-PROD, #153). Every battery case runs through the same
    //! propagation with the linear reference backend and the indexed backend
    //! and requires byte-identical outcomes (canonical schema bytes on
    //! success; identical error variant + message on failure), plus a
    //! property-style generated battery over bounded schemas/plans with a
    //! fixed seed.

    use super::*;
    use crate::lookup::{AuthorizedLookup, OrdinalIndex};
    use std::collections::BTreeMap;

    fn col(i: u128) -> ColumnId {
        ColumnId::from_uuid(uuid::Uuid::from_u128(i))
    }

    fn wide(f: usize) -> LogicalSchema {
        let fields: Vec<LogicalField> = (0..f)
            .map(|i| {
                LogicalField::new(col(i as u128 + 1000), format!("c{i}"), LogicalType::Int64, false)
                    .expect("field")
            })
            .collect();
        LogicalSchema::new(fields).expect("schema")
    }

    fn pred(left: u128, right: u128) -> Expr {
        Expr::Binary {
            left: Box::new(Expr::Column(col(left))),
            operator: stillflow_core::BinaryOperator::GreaterThan,
            right: Box::new(Expr::Column(col(right))),
        }
    }

    fn evaluate(scan_output: &LogicalSchema, steps: &[CompiledStep]) -> Result<Vec<u8>, String> {
        let linear = propagate_schema_with(scan_output, steps, WorkingSchema::linear(scan_output.clone()));
        let indexed = propagate_schema_with(scan_output, steps, WorkingSchema::indexed(scan_output.clone()));
        let produced = propagate_schema(scan_output, steps);
        match (&linear, &indexed, &produced) {
            (Ok(a), Ok(b), Ok(p)) => {
                let (ab, bb, pb) = (
                    a.canonical_bytes().expect("canonical"),
                    b.canonical_bytes().expect("canonical"),
                    p.canonical_bytes().expect("canonical"),
                );
                assert_eq!(ab, bb, "linear vs indexed canonical bytes differ");
                assert_eq!(ab, pb, "production path diverges from both references");
                Ok(ab)
            }
            (Err(a), Err(b), Err(p)) => {
                let fmt = |e: &EngineError| format!("{e:?}");
                assert_eq!(fmt(a), fmt(b), "linear vs indexed error differs (variant+message+payload)");
                assert_eq!(fmt(a), fmt(p), "production path error differs from references");
                Err(fmt(a))
            }
            _ => panic!(
                "backend outcome mismatch: linear={} indexed={} produced={}",
                linear.is_ok(),
                indexed.is_ok(),
                produced.is_ok()
            ),
        }
    }

    /// Runs a battery and returns per-case outcomes for cross-case checks.
    fn battery<S: AsRef<str>>(
        cases: &[(S, LogicalSchema, Vec<CompiledStep>)],
    ) -> BTreeMap<String, String> {
        let mut outcomes = BTreeMap::new();
        for (id, schema, steps) in cases {
            let id = id.as_ref();
            let outcome = match evaluate(schema, steps) {
                Ok(bytes) => format!("ok:{}", hex_fingerprint(&bytes)),
                Err(error) => format!("err:{error}"),
            };
            assert!(
                outcomes.insert(id.to_owned(), outcome.clone()).is_none(),
                "duplicate case id {id}"
            );
            println!("[differential] {id}: {outcome}");
        }
        outcomes
    }

    fn hex_fingerprint(bytes: &[u8]) -> String {
        // First 8 bytes hex: deterministic and tiny.
        bytes.iter().take(8).map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn differential_projection_shapes() {
        let mut cases = Vec::new();
        for f in [64usize, 512, 2048, 4096] {
            for (id, cols) in [
                ("sparse", vec![col(1000), col(1000 + (f as u128 - 1)), col(1000 + f as u128 / 2)]),
                ("first", vec![col(1000)]),
                ("middle", vec![col(1000 + f as u128 / 2)]),
                ("last", vec![col(1000 + f as u128 - 1)]),
                ("mixed", vec![col(1000 + f as u128 - 1), col(1000), col(1000 + f as u128 / 3)]),
            ] {
                cases.push((
                    format!("proj_{id}_f{f}"),
                    wide(f),
                    vec![CompiledStep::Project { columns: cols }],
                ));
            }
        }
        let outcomes = battery(&cases);
        assert_eq!(outcomes.len(), 5 * 4);
    }

    #[test]
    fn differential_unknown_and_duplicate_ids() {
        let schema = wide(64);
        let unknown = col(999_999);
        let cases = vec![
            ("unknown_first", schema.clone(), vec![CompiledStep::Project { columns: vec![unknown, col(1005)] }]),
            ("unknown_middle", schema.clone(), vec![CompiledStep::Project { columns: vec![col(1005), unknown, col(1010)] }]),
            ("unknown_last", schema.clone(), vec![CompiledStep::Project { columns: vec![col(1005), col(1010), unknown] }]),
            ("duplicate_ids", schema.clone(), vec![CompiledStep::Project { columns: vec![col(1005), col(1010), col(1005)] }]),
            ("rule_unknown_rename", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::Rename { column: unknown, to: "x".to_owned() }] }]),
            ("rule_unknown_trim", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::Trim { column: unknown }] }]),
            ("rule_unknown_drop", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::DropColumn { column: unknown }] }]),
        ];
        battery(&cases);
    }

    #[test]
    fn differential_rule_families_and_chains() {
        let schema = wide(32);
        let cast_set_null = Rule::Cast {
            column: col(1003),
            data_type: LogicalType::Float64,
            on_failure: CastFailurePolicy::SetNull,
        };
        let cast_error = Rule::Cast {
            column: col(1004),
            data_type: LogicalType::Float64,
            on_failure: CastFailurePolicy::Error,
        };
        let derive = Rule::DeriveColumn {
            id: col(5000),
            name: "d0".to_owned(),
            data_type: LogicalType::Int64,
            nullable: false,
            expression: Expr::Cast {
                expression: Box::new(Expr::Column(col(1003))),
                data_type: LogicalType::Int64,
            },
        };
        let trim_utf8_ok = {
            // Rename an Int64 column to utf8-typed field via cast first.
            Rule::Cast {
                column: col(1010),
                data_type: LogicalType::Utf8,
                on_failure: CastFailurePolicy::Error,
            }
        };
        let cases = vec![
            ("rename", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::Rename { column: col(1003), to: "renamed".to_owned() }] }]),
            ("drop", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::DropColumn { column: col(1003) }] }]),
            ("trim_type_error", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::Trim { column: col(1003) }] }]),
            ("cast_set_null", schema.clone(), vec![CompiledStep::Rules { rules: vec![cast_set_null] }]),
            ("cast_error", schema.clone(), vec![CompiledStep::Rules { rules: vec![cast_error] }]),
            ("replace_literal", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::ReplaceLiteral { column: col(1005), from: ScalarValue::Int64(1), to: ScalarValue::Int64(2) }] }]),
            ("replace_literal_null", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::ReplaceLiteral { column: col(1005), from: ScalarValue::Int64(1), to: ScalarValue::Null }] }]),
            ("fill_null", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::FillNull { column: col(1005), value: ScalarValue::Int64(0) }] }]),
            ("fill_null_null_value", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::FillNull { column: col(1005), value: ScalarValue::Null }] }]),
            ("derive", schema.clone(), vec![CompiledStep::Rules { rules: vec![derive.clone()] }]),
            ("derive_chain", schema.clone(), vec![CompiledStep::Rules { rules: vec![
                derive.clone(),
                Rule::DeriveColumn {
                    id: col(5001),
                    name: "d1".to_owned(),
                    data_type: LogicalType::Boolean,
                    nullable: false,
                    expression: Expr::Unary {
                        operator: stillflow_core::UnaryOperator::Not,
                        expression: Box::new(Expr::Cast {
                            expression: Box::new(Expr::Column(col(5000))),
                            data_type: LogicalType::Boolean,
                        }),
                    },
                },
            ] }]),
            ("trim_after_cast_to_utf8", schema.clone(), vec![CompiledStep::Rules { rules: vec![
                trim_utf8_ok,
                Rule::Trim { column: col(1010) },
            ] }]),
            ("repeated_filter_refs", wide(16), vec![CompiledStep::Filter { predicate: Expr::Binary {
                left: Box::new(Expr::Column(col(1007))),
                operator: stillflow_core::BinaryOperator::GreaterThan,
                right: Box::new(Expr::Column(col(1007))),
            } }]),
            ("chain_proj_filter_rules_proj", wide(16), vec![
                CompiledStep::Project { columns: vec![col(1004), col(1014), col(1024), col(1034), col(1044)] },
                CompiledStep::Filter { predicate: pred(1004, 1044) },
                CompiledStep::Rules { rules: vec![
                    Rule::Rename { column: col(1004), to: "renamed_a".to_owned() },
                    Rule::Cast { column: col(1014), data_type: LogicalType::Float64, on_failure: CastFailurePolicy::SetNull },
                    Rule::FillNull { column: col(1024), value: ScalarValue::Int64(11) },
                    Rule::ReplaceLiteral { column: col(1034), from: ScalarValue::Int64(1), to: ScalarValue::Int64(2) },
                    Rule::Trim { column: col(1044) }, // type error: int column
                ] },
                CompiledStep::Project { columns: vec![col(1044), col(1004)] },
            ]),
        ];
        battery(&cases);
    }

    #[test]
    fn differential_paused_and_bound_failures() {
        let schema = wide(64);
        let paused_cast = Rule::Cast {
            column: col(1009),
            data_type: LogicalType::Timestamp { unit: stillflow_core::TimeUnit::Second, timezone: None },
            on_failure: CastFailurePolicy::Error,
        };
        let paused_list_derive = Rule::DeriveColumn {
            id: col(6000),
            name: "paused_list".to_owned(),
            data_type: LogicalType::List(Box::new(LogicalType::Int64)),
            nullable: false,
            expression: Expr::Literal(ScalarValue::Null),
        };
        let drop_last = {
            let one = LogicalSchema::new(vec![LogicalField::new(col(1000), "only", LogicalType::Int64, false).expect("f")]).expect("s");
            one
        };
        let cases = vec![
            ("paused_timestamp_second_cast", schema.clone(), vec![CompiledStep::Rules { rules: vec![paused_cast] }]),
            ("paused_list_derive", schema.clone(), vec![CompiledStep::Rules { rules: vec![paused_list_derive] }]),
            ("derive_wrong_type", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::DeriveColumn {
                id: col(6200),
                name: "wrong".to_owned(),
                data_type: LogicalType::Utf8,
                nullable: false,
                expression: Expr::Cast { expression: Box::new(Expr::Column(col(1003))), data_type: LogicalType::Int64 },
            }] }]),
            ("derive_duplicate_name", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::DeriveColumn {
                id: col(6100),
                name: "c3".to_owned(),
                data_type: LogicalType::Int64,
                nullable: false,
                expression: Expr::Cast { expression: Box::new(Expr::Column(col(1003))), data_type: LogicalType::Int64 },
            }] }]),
            ("derive_duplicate_id", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::DeriveColumn {
                id: col(1003),
                name: "fresh".to_owned(),
                data_type: LogicalType::Int64,
                nullable: false,
                expression: Expr::Cast { expression: Box::new(Expr::Column(col(1003))), data_type: LogicalType::Int64 },
            }] }]),
            ("drop_last_column", drop_last, vec![CompiledStep::Rules { rules: vec![Rule::DropColumn { column: col(1000) }] }]),
            ("filter_rows_non_boolean", schema.clone(), vec![CompiledStep::Rules { rules: vec![Rule::FilterRows { predicate: Expr::Literal(ScalarValue::Int64(1)) }] }]),
        ];
        battery(&cases);
    }

    #[test]
    fn differential_projected_schema_equality() {
        // project_schema itself: linear reference vs forced indexed lookup vs
        // the production single-call policy.
        for f in [64usize, 512, 2048, 4096] {
            let schema = wide(f);
            for (id, cols) in [
                ("sparse", vec![col(1000), col(1000 + f as u128 / 2), col(1000 + f as u128 - 1)]),
                ("dense", (0..f).map(|i| col(i as u128 + 1000)).collect()),
            ] {
                let linear = project_schema_with(&schema, &cols).expect("linear");
                let indexed = project_schema_with(&AuthorizedLookup::Indexed(OrdinalIndex::build(&schema)), &cols).expect("indexed");
                let produced = project_schema(&schema, &cols).expect("produced");
                assert_eq!(
                    linear.canonical_bytes().expect("canonical"),
                    indexed.canonical_bytes().expect("canonical"),
                    "{id} f{f}: linear vs indexed"
                );
                assert_eq!(
                    linear.canonical_bytes().expect("canonical"),
                    produced.canonical_bytes().expect("canonical"),
                    "{id} f{f}: production"
                );
            }
            // Unknown column error parity at the projection level.
            let unknown = col(999_999);
            let e1 = project_schema_with(&schema, &[unknown]).expect_err("linear unknown");
            let e2 = project_schema_with(&AuthorizedLookup::Indexed(OrdinalIndex::build(&schema)), &[unknown]).expect_err("indexed unknown");
            assert_eq!(format!("{e1:?}"), format!("{e2:?}"));
        }
    }

    #[test]
    fn differential_property_generated_battery() {
        // Fixed-seed deterministic generator over bounded schemas/plans.
        // Every generated case compares linear vs indexed byte-identically.
        // On failure the seed and case are printed (reproducible).
        let seed = 0x5eed_1530_0b2a_1f41u64;
        let mut state = seed;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for case_index in 0..240 {
            let f = (next() % 48 + 1) as usize; // 1..=48 fields
            let schema = wide(f);
            let step_count = (next() % 6) as usize; // 0..=5 steps
            let mut steps = Vec::new();
            for _ in 0..step_count {
                let kind = next() % 4;
                match kind {
                    0 => {
                        // Project 1..=f columns, ids within range; 5% unknown.
                        let k = (next() % (f as u64) + 1) as usize;
                        let mut cols = Vec::new();
                        for _ in 0..k {
                            let id: u128 = if next() % 20 == 0 {
                                999_999 + (next() % 5) as u128
                            } else {
                                1000 + (next() % (f as u64)) as u128
                            };
                            cols.push(col(id));
                        }
                        steps.push(CompiledStep::Project { columns: cols });
                    }
                    1 => {
                        let a: u128 = 1000 + (next() % (f as u64)) as u128;
                        let b: u128 = 1000 + (next() % (f as u64)) as u128;
                        steps.push(CompiledStep::Filter { predicate: pred(a, b) });
                    }
                    2 => {
                        let column: u128 = 1000 + (next() % (f as u64)) as u128;
                        match next() % 5 {
                            0 => steps.push(CompiledStep::Rules { rules: vec![Rule::Rename { column: col(column), to: format!("r{case_index}") }] }),
                            1 => {
                                if next() % 3 == 0 {
                                    steps.push(CompiledStep::Rules { rules: vec![Rule::DropColumn { column: col(999_997 + (next() % 3) as u128) }] });
                                } else {
                                    steps.push(CompiledStep::Rules { rules: vec![Rule::DropColumn { column: col(column) }] });
                                }
                            }
                            2 => steps.push(CompiledStep::Rules { rules: vec![Rule::Trim { column: col(column) }] }),
                            3 => steps.push(CompiledStep::Rules { rules: vec![Rule::FillNull { column: col(column), value: ScalarValue::Int64(if next() % 3 == 0 { 0 } else { 7 }) }] }),
                            _ => steps.push(CompiledStep::Rules { rules: vec![Rule::Cast { column: col(column), data_type: LogicalType::Float64, on_failure: CastFailurePolicy::Error }] }),
                        }
                    }
                    _ => {
                        // FilterRows with a random column predicate.
                        let a: u128 = 1000 + (next() % (f as u64)) as u128;
                        let b: u128 = 1000 + (next() % (f as u64)) as u128;
                        steps.push(CompiledStep::Rules { rules: vec![Rule::FilterRows { predicate: pred(a, b) }] });
                    }
                }
            }
            let _ = match evaluate(&schema, &steps) {
                Ok(bytes) => (case_index, bytes.len()),
                Err(error) => {
                    println!("[generated] case {case_index} seed {seed}: f={f} steps={step_count} -> err {error}");
                    (case_index, 0)
                }
            };
            // The policy routing must also be consistent (determinism).
            assert_eq!(
                crate::lookup::use_index(schema.fields.len(), steps_served_lookups(&steps)),
                crate::lookup::use_index(schema.fields.len(), steps_served_lookups(&steps)),
                "policy must be a pure function"
            );
        }
    }
}
