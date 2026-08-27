use polars::prelude::{col, lit, when, DataFrame, Expr as PolarsExpr, IntoLazy, NULL};
use stillflow_core::{
    BinaryOperator, ColumnId, Expr, LogicalField, LogicalSchema, LogicalType, ScalarValue,
    UnaryOperator,
};
use stillflow_plan::{CastFailurePolicy, Rule};

use crate::error::EngineError;
use crate::preflight::CompiledStep;
use crate::types::polars_data_type;

/// Cached entry point for the engine chunk loop (feature ON): one
/// LoweringCache instance is owned by the run and passed by &mut on every
/// chunk. Feature OFF callers use `transform` and never construct a cache.
#[cfg(feature = "engine-lowering-cache")]
pub(crate) fn transform_cached<C: CacheSurface>(
    frame: DataFrame,
    schema: &LogicalSchema,
    steps: &[CompiledStep],
    cache: &mut C,
) -> Result<(DataFrame, Vec<(String, ScalarValue)>), EngineError> {
    transform_steps(frame, schema, steps, Vec::new(), cache)
}

pub(crate) fn transform(
    frame: DataFrame,
    schema: &LogicalSchema,
    steps: &[CompiledStep],
) -> Result<(DataFrame, Vec<(String, ScalarValue)>), EngineError> {
    #[cfg(feature = "engine-lowering-cache")]
    {
        let mut cache = LoweringCache::new();
        transform_steps(frame, schema, steps, Vec::new(), &mut cache)
    }
    #[cfg(not(feature = "engine-lowering-cache"))]
    transform_steps(frame, schema, steps, Vec::new(), &mut NoCache)
}

/// Per-run lowering/type-check cache experiment (O0-B1-A1, issue #147).
///
/// Non-global: the engine chunk loop owns one instance for the whole run, so
/// entries can never escape the run or be reused across unrelated plans. Key
/// identity = exact compiled step/rule position PLUS exact logical-schema
/// state fingerprint (field id/name/type/nullability/count fed to a 64-bit
/// hasher — intra-run uniqueness is all that is required, so the dependency-
/// free std hasher suffices). Lazy first-use preserves observable failure
/// timing exactly: only SUCCESSFUL lowerings are cached, and a first failure
/// surfaces at the same step/rule as feature OFF.
#[cfg(feature = "engine-lowering-cache")]
pub(crate) struct LoweringCache {
    lowered: std::collections::HashMap<(usize, usize, u64), PolarsExpr>,
    counter_hits: u64,
    counter_misses: u64,
}

#[cfg(feature = "engine-lowering-cache")]
impl Drop for LoweringCache {
    fn drop(&mut self) {
        // Evidence-only trace for benchmark runs (issue #147 construction-
        // counter requirement); enabled by LOWERING_CACHE_TRACE=1.
        if std::env::var("LOWERING_CACHE_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "lowering_cache_counters hits={} misses={}",
                self.counter_hits, self.counter_misses
            );
        }
    }
}

#[cfg(feature = "engine-lowering-cache")]
impl LoweringCache {
    pub(crate) fn new() -> Self {
        Self {
            lowered: std::collections::HashMap::new(),
            counter_hits: 0,
            counter_misses: 0,
        }
    }

    /// Evidence-only counters (hits, misses): how many lowering constructions
    /// were served from cache vs rebuilt. Must never become a production
    /// public metric (issue #147 contract). Unused outside cfg(test) today;
    /// the benchmark harness reads it through a test-only accessor.
    #[allow(dead_code)]
    pub(crate) fn construction_counters(&self) -> (u64, u64) {
        (self.counter_hits, self.counter_misses)
    }
}

/// OFF-path no-op surface; only constructed when the feature is disabled.
#[cfg(not(feature = "engine-lowering-cache"))]
pub(crate) struct NoCache;

pub(crate) trait CacheSurface {
    fn lookup_lowered(&mut self, key: (usize, usize, u64)) -> Option<PolarsExpr>;
    fn store_lowered(&mut self, key: (usize, usize, u64), expr: PolarsExpr);
    fn record_hit(&mut self);
    fn record_miss(&mut self);
}

#[cfg(feature = "engine-lowering-cache")]
impl CacheSurface for LoweringCache {
    fn lookup_lowered(&mut self, key: (usize, usize, u64)) -> Option<PolarsExpr> {
        self.lowered.get(&key).cloned()
    }
    fn store_lowered(&mut self, key: (usize, usize, u64), expr: PolarsExpr) {
        self.lowered.insert(key, expr);
    }
    fn record_hit(&mut self) {
        self.counter_hits += 1;
    }
    fn record_miss(&mut self) {
        self.counter_misses += 1;
    }
}

#[cfg(not(feature = "engine-lowering-cache"))]
impl CacheSurface for NoCache {
    fn lookup_lowered(&mut self, _key: (usize, usize, u64)) -> Option<PolarsExpr> {
        None
    }
    fn store_lowered(&mut self, _key: (usize, usize, u64), _expr: PolarsExpr) {}
    fn record_hit(&mut self) {}
    fn record_miss(&mut self) {}
}

/// Exact schema-state fingerprint + position key. Position is (step index,
/// rule index within its Rules step); the fingerprint hashes every field's
/// id/name/data_type/nullable plus the field count, so ANY schema mutation
/// changes the key and stale reuse across schema states is impossible.
fn cache_key(schema: &LogicalSchema, step_index: usize, rule_index: usize) -> (usize, usize, u64) {
    use std::hash::Hash;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for field in &schema.fields {
        field.id.hash(&mut hasher);
        field.name.hash(&mut hasher);
        format!("{:?}", field.data_type).hash(&mut hasher);
        field.nullable.hash(&mut hasher);
    }
    schema.fields.len().hash(&mut hasher);
    (step_index, rule_index, std::hash::Hasher::finish(&hasher))
}

fn transform_steps<C: CacheSurface>(
    mut frame: DataFrame,
    schema: &LogicalSchema,
    steps: &[CompiledStep],
    mut deferred: Vec<(String, ScalarValue)>,
    cache: &mut C,
) -> Result<(DataFrame, Vec<(String, ScalarValue)>), EngineError> {
    let mut schema = schema.clone();
    for (step_index, step) in steps.iter().enumerate() {
        match step {
            CompiledStep::Project { columns } => {
                let names = names_for(&schema, columns)?;
                frame = frame
                    .select(names.iter().map(String::as_str))
                    .map_err(|_| EngineError::UnknownColumn(columns[0]))?;
                deferred.retain(|(name, _)| names.iter().any(|keep| keep == name));
                schema = crate::preflight::project_schema(&schema, columns)?;
            }
            CompiledStep::Filter { predicate } => {
                let key = cache_key(&schema, step_index, 0);
                let expr = match cache.lookup_lowered(key) {
                    Some(expr) => {
                        cache.record_hit();
                        expr
                    }
                    None => {
                        cache.record_miss();
                        let expr = lower_expr(predicate, &schema)?;
                        cache.store_lowered(key, expr.clone());
                        expr
                    }
                };
                frame = frame
                    .lazy()
                    .filter(expr)
                    .collect()
                    .map_err(|_| EngineError::TypeError("filter evaluation failed"))?;
            }
            CompiledStep::Rules { rules } => {
                for (rule_index, rule) in rules.iter().enumerate() {
                    frame = apply_rule_cached(
                        frame,
                        &mut schema,
                        &mut deferred,
                        rule,
                        cache,
                        step_index,
                        rule_index,
                    )?;
                }
            }
        }
    }
    Ok((frame, deferred))
}

/// Cached-rule wrapper (feature ON): only DeriveColumn with a non-literal
/// expression actually performs lowering/type-check work today; all other
/// rules resolve names (O(fields), not worth caching) and are delegated
/// unchanged. Keying includes the rule index so identical rule bodies at
/// different positions never share an entry.
fn apply_rule_cached<C: CacheSurface>(
    frame: DataFrame,
    schema: &mut LogicalSchema,
    deferred: &mut Vec<(String, ScalarValue)>,
    rule: &Rule,
    cache: &mut C,
    step_index: usize,
    rule_index: usize,
) -> Result<DataFrame, EngineError> {
    if let Rule::DeriveColumn {
        id,
        name,
        data_type,
        nullable,
        expression,
    } = rule
    {
        let key = cache_key(schema, step_index, 128 + rule_index);
        let expr = match cache.lookup_lowered(key) {
            Some(expr) => {
                cache.record_hit();
                expr
            }
            None => {
                cache.record_miss();
                // Type-check + lower exactly as the uncached derive path does;
                // on success both products become one cached entry.
                let _checked = crate::typing::type_check_expr(expression, schema)?;
                let expr = lower_expr(expression, schema)?;
                cache.store_lowered(key, expr.clone());
                expr
            }
        };
        let dtype = polars_data_type(data_type)?;
        let mut derived = frame;
        derived = derived
            .lazy()
            .with_column(expr.cast(dtype.clone()).alias(name.as_str()))
            .collect()
            .map_err(|_| EngineError::TypeError("derive-column failed"))?;
        let mut fields = schema.fields.clone();
        fields.push(
            LogicalField::new(*id, name.clone(), data_type.clone(), *nullable)
                .map_err(|_| EngineError::InvalidPlan("derived field is invalid"))?,
        );
        *schema = LogicalSchema::new(fields)
            .map_err(|_| EngineError::InvalidPlan("derive produced an invalid schema"))?;
        return Ok(derived);
    }
    apply_rule(frame, schema, deferred, rule)
}

fn apply_rule(
    frame: DataFrame,
    schema: &mut LogicalSchema,
    deferred: &mut Vec<(String, ScalarValue)>,
    rule: &Rule,
) -> Result<DataFrame, EngineError> {
    match rule {
        Rule::Rename { column, to } => {
            let from = field_name(schema, *column)?;
            for (name, _) in deferred.iter_mut() {
                if name == &from {
                    *name = to.clone();
                }
            }
            let renamed = frame
                .lazy()
                .rename([from.as_str()], [to.as_str()], true)
                .collect()
                .map_err(|_| EngineError::Internal("rename failed"))?;
            schema
                .rename_column(*column, to.clone())
                .map_err(|_| EngineError::UnknownColumn(*column))?;
            Ok(renamed)
        }
        Rule::DropColumn { column } => {
            let name = field_name(schema, *column)?;
            deferred.retain(|(deferred_name, _)| deferred_name != &name);
            let dropped = frame
                .drop(name.as_str())
                .map_err(|_| EngineError::UnknownColumn(*column))?;
            let keep: Vec<ColumnId> = schema
                .fields
                .iter()
                .filter(|field| field.id != *column)
                .map(|field| field.id)
                .collect();
            *schema = crate::preflight::project_schema(schema, &keep)?;
            Ok(dropped)
        }
        Rule::Trim { column } => {
            let name = field_name(schema, *column)?;
            frame
                .lazy()
                .with_column(
                    col(name.as_str())
                        .str()
                        .strip_chars(lit(NULL))
                        .alias(name.as_str()),
                )
                .collect()
                .map_err(|_| EngineError::TypeError("trim failed"))
        }
        Rule::DeriveColumn {
            id,
            name,
            data_type,
            nullable,
            expression,
        } => {
            let derived = match expression {
                Expr::Literal(value)
                    if matches!(data_type, LogicalType::Utf8)
                        && matches!(value, ScalarValue::Utf8(_) | ScalarValue::Null) =>
                {
                    let height = frame.height();
                    let mut derived = frame;
                    let dtype = polars_data_type(data_type)?;
                    derived
                        .with_column(polars::prelude::Column::full_null(
                            name.as_str().into(),
                            height,
                            &dtype,
                        ))
                        .map_err(|_| EngineError::TypeError("derive-column failed"))?;
                    deferred.push((name.clone(), value.clone()));
                    derived
                }
                Expr::Literal(ScalarValue::Null) => {
                    let height = frame.height();
                    let mut derived = frame;
                    let dtype = polars_data_type(data_type)?;
                    derived
                        .with_column(polars::prelude::Column::full_null(
                            name.as_str().into(),
                            height,
                            &dtype,
                        ))
                        .map_err(|_| EngineError::TypeError("derive-column failed"))?;
                    derived
                }
                Expr::Literal(value) => {
                    let height = frame.height();
                    let mut derived = frame;
                    derived
                        .with_column(polars::prelude::Column::new_scalar(
                            name.as_str().into(),
                            literal_scalar(value)?,
                            height,
                        ))
                        .map_err(|_| EngineError::TypeError("derive-column failed"))?;
                    derived
                }
                _ => {
                    let expr = lower_expr(expression, schema)?;
                    let dtype = polars_data_type(data_type)?;
                    frame
                        .lazy()
                        .with_column(expr.cast(dtype).alias(name.as_str()))
                        .collect()
                        .map_err(|_| EngineError::TypeError("derive-column failed"))?
                }
            };
            let mut fields = schema.fields.clone();
            fields.push(
                LogicalField::new(*id, name.clone(), data_type.clone(), *nullable)
                    .map_err(|_| EngineError::InvalidPlan("derived field is invalid"))?,
            );
            *schema = LogicalSchema::new(fields)
                .map_err(|_| EngineError::InvalidPlan("derive produced an invalid schema"))?;
            Ok(derived)
        }
        Rule::ReplaceLiteral { column, from, to } => {
            let name = field_name(schema, *column)?;
            let expr = match from {
                ScalarValue::Null => col(name.as_str()).fill_null(literal(to)?),
                _ => when(col(name.as_str()).eq(literal(from)?))
                    .then(literal(to)?)
                    .otherwise(col(name.as_str())),
            };
            frame
                .lazy()
                .with_column(expr.alias(name.as_str()))
                .collect()
                .map_err(|_| EngineError::TypeError("replace-literal failed"))
        }
        Rule::FillNull { column, value } => {
            let name = field_name(schema, *column)?;
            frame
                .lazy()
                .with_column(
                    col(name.as_str())
                        .fill_null(literal(value)?)
                        .alias(name.as_str()),
                )
                .collect()
                .map_err(|_| EngineError::TypeError("fill-null failed"))
        }
        Rule::Cast {
            column,
            data_type,
            on_failure,
        } => {
            let name = field_name(schema, *column)?;
            let dtype = polars_data_type(data_type)?;
            let expr = if matches!(on_failure, CastFailurePolicy::Error) {
                col(name.as_str()).strict_cast(dtype.clone())
            } else {
                col(name.as_str()).cast(dtype)
            };
            frame
                .lazy()
                .with_column(expr.alias(name.as_str()))
                .collect()
                .map_err(|_| EngineError::CastFailure {
                    column: *column,
                    sequence: 0,
                    row: 0,
                })
        }
        Rule::FilterRows { predicate } => {
            let expr = lower_expr(predicate, schema)?;
            frame
                .lazy()
                .filter(expr)
                .collect()
                .map_err(|_| EngineError::TypeError("filter-rows failed"))
        }
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

fn names_for(schema: &LogicalSchema, columns: &[ColumnId]) -> Result<Vec<String>, EngineError> {
    columns.iter().map(|id| field_name(schema, *id)).collect()
}

fn field_name(schema: &LogicalSchema, id: ColumnId) -> Result<String, EngineError> {
    schema
        .field(id)
        .map(|field| field.name.clone())
        .ok_or(EngineError::UnknownColumn(id))
}

fn lower_expr(expr: &Expr, schema: &LogicalSchema) -> Result<PolarsExpr, EngineError> {
    Ok(match expr {
        Expr::Column(id) => col(field_name(schema, *id)?),
        Expr::Literal(value) => literal(value)?,
        Expr::Unary {
            operator: UnaryOperator::Not,
            expression,
        } => lower_expr(expression, schema)?.not(),
        Expr::Unary {
            operator: UnaryOperator::Negate,
            ..
        } => {
            return Err(EngineError::TypeError(
                "checked arithmetic is paused until overflow semantics are implemented",
            ));
        }
        Expr::Binary {
            left,
            operator,
            right,
        } => {
            let left_type = crate::typing::type_check_expr(left, schema)?;
            let right_type = crate::typing::type_check_expr(right, schema)?;
            let mut left_expr = lower_expr(left, schema)?;
            let mut right_expr = lower_expr(right, schema)?;
            if left_type != right_type {
                let lub = left_type
                    .least_upper_bound(&right_type)
                    .map_err(|_| EngineError::TypeError("incompatible binary operand types"))?;
                if left_type != lub {
                    let lub_dtype = polars_data_type(&lub)?;
                    left_expr = left_expr.strict_cast(lub_dtype);
                }
                if right_type != lub {
                    let lub_dtype = polars_data_type(&lub)?;
                    right_expr = right_expr.strict_cast(lub_dtype);
                }
            }
            match operator {
                BinaryOperator::Equal => left_expr.eq(right_expr),
                BinaryOperator::NotEqual => left_expr.neq(right_expr),
                BinaryOperator::LessThan => left_expr.lt(right_expr),
                BinaryOperator::LessThanOrEqual => left_expr.lt_eq(right_expr),
                BinaryOperator::GreaterThan => left_expr.gt(right_expr),
                BinaryOperator::GreaterThanOrEqual => left_expr.gt_eq(right_expr),
                BinaryOperator::And => left_expr.and(right_expr),
                BinaryOperator::Or => left_expr.or(right_expr),
                BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => {
                    return Err(EngineError::TypeError(
                        "checked arithmetic is paused until overflow semantics are implemented",
                    ));
                }
                BinaryOperator::Contains => {
                    return Err(EngineError::TypeError(
                        "contains is paused until the regex polars feature is approved",
                    ));
                }
            }
        }
        Expr::IsNull {
            expression,
            negated,
        } => {
            let inner = lower_expr(expression, schema)?;
            if *negated {
                inner.is_not_null()
            } else {
                inner.is_null()
            }
        }
        Expr::Cast {
            expression,
            data_type,
        } => lower_expr(expression, schema)?.strict_cast(polars_data_type(data_type)?),
        Expr::Coalesce { expressions } => {
            if expressions.is_empty() {
                return Ok(lit(NULL));
            }
            let target_lub = crate::typing::type_check_expr(expr, schema)?;
            let target_dtype = polars_data_type(&target_lub)?;
            let mut lowered = Vec::new();
            for e in expressions {
                let arm_type = crate::typing::type_check_expr(e, schema)?;
                let mut arm_expr = lower_expr(e, schema)?;
                if arm_type != target_lub {
                    arm_expr = arm_expr.strict_cast(target_dtype.clone());
                }
                lowered.push(arm_expr);
            }
            coalesce_exprs(lowered)
        }
    })
}

fn coalesce_exprs(mut exprs: Vec<PolarsExpr>) -> PolarsExpr {
    let Some(first) = exprs.pop() else {
        return lit(NULL);
    };
    exprs.into_iter().rev().fold(first, |tail, head| {
        when(head.clone().is_not_null()).then(head).otherwise(tail)
    })
}

fn literal(value: &ScalarValue) -> Result<PolarsExpr, EngineError> {
    Ok(match value {
        ScalarValue::Null => lit(NULL),
        ScalarValue::Boolean(value) => lit(*value),
        ScalarValue::Int64(value) => lit(*value),
        ScalarValue::UInt64(value) => lit(*value),
        ScalarValue::Float64(value) => lit(value.get()),
        ScalarValue::Utf8(value) => lit(value.clone()),
    })
}

fn literal_scalar(value: &ScalarValue) -> Result<polars::prelude::Scalar, EngineError> {
    use polars::prelude::{AnyValue, DataType, Scalar};
    Ok(match value {
        ScalarValue::Null => Scalar::new(DataType::Null, AnyValue::Null),
        ScalarValue::Boolean(value) => Scalar::from(*value),
        ScalarValue::Int64(value) => Scalar::from(*value),
        ScalarValue::UInt64(value) => Scalar::from(*value),
        ScalarValue::Float64(value) => Scalar::from(value.get()),
        ScalarValue::Utf8(value) => Scalar::from(polars::prelude::PlSmallStr::from(value.as_str())),
    })
}
