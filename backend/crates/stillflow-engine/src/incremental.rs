//! Engine-private incremental schema-mutation layer (O0-B2-A2 production, #159),
//! extended for the O0-B2-A1-A2 four-cell attribution (#163): the layer also
//! carries the accepted A1 ordinal-index lookup capability (lookup.rs) when
//! the deterministic A1 `use_index` policy says indexing is warranted, and
//! every accepted mutation rederives the ordinal table from the exact
//! post-mutation field list in the same call, so no stale-index window can
//! exist between A2 in-place mutation and a subsequent A1 indexed lookup.
//!
//! Ownership model: every per-rule schema transition under `ApplyRules` flows
//! through [`IncrementalSchema`] — the ONE Engine-private layer that:
//!
//! 1. performs the exact legacy per-rule precondition checks in the exact
//!    order, with identical error variants and stable messages;
//! 2. replicates the core schema **budget accounting** (field count and
//!    cumulative text bytes: field names + field metadata + timestamp
//!    timezone text) so budget failures surface at the SAME rule position as
//!    the legacy per-rule full rebuild — never deferred to a later boundary
//!    error — with the legacy error mapping (e.g. rename budget crossings
//!    surface as `UnknownColumn`, exactly as the legacy catch-all does);
//! 3. mutates only the affected field(s) in place, eliminating the legacy
//!    per-rule `fields.clone()` + `LogicalSchema::new` reconstruction.
//!
//! The full [`LogicalSchema::new`] validation remains a **permanent
//! production safety oracle** at the ApplyRules boundary
//! ([`IncrementalSchema::into_schema`]); it is a last-resort invariant check,
//! not a substitute for exact per-rule error fidelity.
//!
//! Anti-drift: the rule mapping below is **exhaustive** (no wildcard arm) —
//! adding a new `Rule` variant is a compile error until this layer and its
//! batteries are extended. The differential/property batteries (random
//! module) mechanically compare this layer against the legacy full-rebuild
//! path on every surface including all budget edges with a fixed seed and
//! print seed/case/rule on divergence; the budget accounting is additionally
//! pinned by an exactness test against a fresh core-style recount.

use stillflow_core::{
    LogicalField, LogicalSchema, LogicalType, ScalarValue, TimeUnit, MAX_SCHEMA_FIELDS,
    MAX_SCHEMA_TEXT_BYTES,
};
use stillflow_plan::{CastFailurePolicy, Rule};

use crate::error::EngineError;
use crate::lookup::{build_ordinals, use_index, ColumnLookup};
use crate::preflight::{
    infer_nullability, reject_paused_cast, reject_paused_cast_in_expr, validate_expr,
    validate_literal_for_column,
};
use crate::typing::{reject_paused_type, require_boolean_in, type_check_expr_in};

/// Cumulative text accounting contributed by one field, replicating the core
/// `validate_nodes` budget rules for the reachable preflight state (flat
/// fields; List/Struct are paused engine-wide, so no nested text exists): the
/// field name, its metadata keys+values, and the timestamp timezone text when
/// present.
fn field_text(field: &LogicalField) -> usize {
    let mut bytes = field.name.len();
    for (key, value) in &field.metadata {
        bytes += key.len() + value.len();
    }
    bytes
}

fn type_timezone_text(data_type: &LogicalType) -> usize {
    match data_type {
        LogicalType::Timestamp {
            unit:
                TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            timezone: Some(timezone),
        } => timezone.len(),
        _ => 0,
    }
}

/// Exact cumulative text-budget accounting over the core validation rules:
/// names + metadata (per field, in field order), then timestamp timezone
/// text (the core walks types in reverse field order, but the sum is
/// order-independent).
fn text_bytes_of(schema: &LogicalSchema) -> usize {
    let mut bytes = 0_usize;
    for field in &schema.fields {
        bytes += field_text(field);
        bytes += type_timezone_text(&field.data_type);
    }
    bytes
}

/// The production incremental mutation layer. Fields are authoritative and
/// mutated in place; `text_bytes` mirrors the core cumulative text budget so
/// budget crossings are detected at the exact mutating rule.
///
/// A1A2 integration (#163): when `entries` is `Some`, `ColumnId` resolution
/// goes through the A1 ordinal table (ordinal -> field position) instead of a
/// linear scan, exactly as the accepted A1 lookup semantics define. The
/// table is a deterministic image of the current validated field list (ids
/// sorted, first-wins). It is rederived in the same call only for mutations
/// that can change the ColumnId -> ordinal mapping (DropColumn, DeriveColumn,
/// swap_with); Rename/Cast/ReplaceLiteral/FillNull change only name/type/
/// nullability, which the ordinal table does not depend on, so the held
/// entries remain the exact deterministic image (mechanically guarded by
/// verify_entries after every rule). The rebuild happens before the method
/// returns, so no stale window can exist.
pub(crate) struct IncrementalSchema {
    schema: LogicalSchema,
    text_bytes: usize,
    /// A1 ordinal table for indexed lookups (`None` = linear reference
    /// semantics). Remains `None`/`Some` across projection swaps and rule
    /// mutations; rebuilt in the same call as every accepted mutation that
    /// can change the ColumnId -> ordinal mapping (drop/derive/swap).
    entries: Option<Vec<(stillflow_core::ColumnId, u32)>>,
}

impl IncrementalSchema {
    /// Linear-reference working schema (exactly the A2 production behavior).
    /// Test-only in the final combination: production uses `for_shape`.
    #[cfg(test)]
    pub(crate) fn from_schema(schema: LogicalSchema) -> Self {
        let text_bytes = text_bytes_of(&schema);
        Self {
            schema,
            text_bytes,
            entries: None,
        }
    }

    /// Deterministic A1 policy choice for an owned schema and the exact
    /// number of lookups the propagation pass will serve (A1 semantics
    /// unchanged: index iff `use_index(fields, served_lookups)`).
    pub(crate) fn for_shape(schema: LogicalSchema, served_lookups: usize) -> Self {
        let text_bytes = text_bytes_of(&schema);
        let entries = if use_index(schema.fields.len(), served_lookups) {
            Some(build_ordinals(&schema))
        } else {
            None
        };
        Self {
            schema,
            text_bytes,
            entries,
        }
    }

    /// Adopts an already-validated schema wholesale, preserving the current
    /// indexed/linear decision (A1 `WorkingSchema::swap_with` semantics:
    /// rederives the ordinal table from the adopted state in the same call).
    pub(crate) fn swap_with(self, schema: LogicalSchema) -> Self {
        let mut working = Self {
            schema,
            text_bytes: 0,
            entries: self.entries,
        };
        working.text_bytes = text_bytes_of(&working.schema);
        working.refresh();
        working
    }

    /// Indexed backend for tests: forces the ordinal table regardless of the
    /// deterministic policy (A1 `WorkingSchema::indexed` analogue).
    #[cfg(test)]
    pub(crate) fn indexed(schema: LogicalSchema) -> Self {
        let mut working = Self::from_schema(schema);
        working.entries = Some(build_ordinals(&working.schema));
        working
    }

    /// Rebuilds the ordinal table from the current schema state (indexed
    /// variant only). Called at the end of every accepted mutation, in the
    /// same call, so the index can never outlive the schema state it was
    /// derived from (A1 refresh semantics).
    fn refresh(&mut self) {
        if let Some(entries) = self.entries.as_mut() {
            *entries = build_ordinals(&self.schema);
        }
    }

    /// Recomputes the ordinal table and compares it with the held entries;
    /// `false` means the index is inconsistent with its schema (unreachable
    /// by construction, mechanically guarded).
    #[cfg(test)]
    pub(crate) fn verify_entries(&self) -> bool {
        match &self.entries {
            None => true,
            Some(entries) => build_ordinals(&self.schema) == *entries,
        }
    }

    /// Exact ordinal of `id` in the current field list, through the index
    /// when indexed (same first-wins semantics as the linear scan).
    #[inline]
    fn ordinal_of(&self, column: stillflow_core::ColumnId) -> Option<usize> {
        match &self.entries {
            Some(entries) => entries
                .binary_search_by_key(&column, |(key, _)| *key)
                .ok()
                .map(|position| entries[position].1 as usize),
            None => self
                .schema
                .fields
                .iter()
                .position(|field| field.id == column),
        }
    }

    /// Applies one rule with legacy-equivalent preconditions and in-place
    /// mutation. Exhaustive over all `Rule` variants: a new variant cannot
    /// compile without extending this match (anti-drift requirement).
    ///
    /// O0-B2-A1-A2-FINAL-INTEGRATION (#166) mutation classification — every
    /// current Rule variant falls in exactly one class (a new variant cannot
    /// compile without being classified here; no wildcard arm):
    ///
    /// - non-mutating (no schema/index change, no rebuild): Trim, FilterRows,
    ///   Validate, Deduplicate;
    /// - schema-mutating but ordinal-preserving (entries stay exact without
    ///   rebuild by construction — entries depend only on ColumnId+position;
    ///   Rename changes name, Cast type/nullability, ReplaceLiteral and
    ///   FillNull nullability): Rename, Cast, ReplaceLiteral, FillNull;
    /// - schema-mutating and ordinal-shifting (same-call rebuild before
    ///   return): DropColumn (retain shifts positions), DeriveColumn
    ///   (append), and `swap_with` (projection replaces the field list).
    pub(crate) fn apply_rule(&mut self, rule: &Rule) -> Result<(), EngineError> {
        match rule {
            Rule::Rename { column, to } => self.apply_rename(*column, to),
            Rule::DropColumn { column } => self.apply_drop(*column),
            Rule::Trim { column } => {
                let field = self
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
            } => self.apply_cast(*column, data_type, *on_failure),
            Rule::ReplaceLiteral { column, from, to } => {
                let field = self
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
                    if let Some(field) = self
                        .schema
                        .fields
                        .iter_mut()
                        .find(|field| field.id == *column)
                    {
                        field.nullable = true;
                    }
                }
                // Option 3 (#166): replace-literal changes nullability only.
                Ok(())
            }
            Rule::FillNull { column, value } => {
                if matches!(value, ScalarValue::Null) {
                    return Err(EngineError::TypeError("fill-null value must not be null"));
                }
                let field = self
                    .lookup_field(*column)
                    .ok_or(EngineError::UnknownColumn(*column))?;
                if matches!(field.data_type, LogicalType::Binary) {
                    return Err(EngineError::TypeError(
                        "fill-null is not authorized on binary",
                    ));
                }
                validate_literal_for_column(&field.data_type, value)?;
                if let Some(field) = self
                    .schema
                    .fields
                    .iter_mut()
                    .find(|field| field.id == *column)
                {
                    field.nullable = false;
                }
                // Option 3 (#166): fill-null changes nullability only.
                Ok(())
            }
            Rule::DeriveColumn {
                id,
                name,
                data_type,
                nullable,
                expression,
            } => self.apply_derive(*id, name, data_type, *nullable, expression),
            Rule::FilterRows { predicate } => {
                require_boolean_in(predicate, &*self)?;
                Ok(())
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

    fn apply_rename(
        &mut self,
        column: stillflow_core::ColumnId,
        to: &str,
    ) -> Result<(), EngineError> {
        let index = self
            .ordinal_of(column)
            .ok_or(EngineError::UnknownColumn(column))?;
        let old_name = self.schema.fields[index].name.clone();
        // Legacy maps EVERY core-level rename rejection (unknown id, empty or
        // whitespace-only name, duplicate name, and cumulative text-budget
        // crossing) to UnknownColumn(column); the budget accounting is
        // replicated here so the failure lands at the same rule position.
        let new_total = self
            .text_bytes
            .checked_sub(old_name.len())
            .and_then(|base| base.checked_add(to.len()))
            .unwrap_or(MAX_SCHEMA_TEXT_BYTES + 1);
        if new_total > MAX_SCHEMA_TEXT_BYTES {
            return Err(EngineError::UnknownColumn(column));
        }
        if to.trim().is_empty()
            || self
                .schema
                .fields
                .iter()
                .any(|field| field.id != column && field.name == to)
        {
            return Err(EngineError::UnknownColumn(column));
        }
        self.schema.fields[index].name = to.to_owned();
        self.text_bytes = new_total;
        // Option 3 (#166): rename cannot change the ColumnId -> ordinal
        // mapping, so entries stay exact without a rebuild (verify_entries-
        // guarded; identical to a fresh rebuild by construction).
        Ok(())
    }

    fn apply_drop(&mut self, column: stillflow_core::ColumnId) -> Result<(), EngineError> {
        if self.schema.fields.len() <= 1 {
            return Err(EngineError::InvalidPlan(
                "cannot drop the last remaining column",
            ));
        }
        let before = self.schema.fields.len();
        let mut removed_text = 0_usize;
        self.schema.fields.retain(|field| {
            if field.id == column {
                removed_text += field_text(field) + type_timezone_text(&field.data_type);
                false
            } else {
                true
            }
        });
        if self.schema.fields.len() == before {
            return Err(EngineError::UnknownColumn(column));
        }
        self.text_bytes = self.text_bytes.saturating_sub(removed_text);
        self.refresh();
        Ok(())
    }

    fn apply_cast(
        &mut self,
        column: stillflow_core::ColumnId,
        data_type: &LogicalType,
        on_failure: CastFailurePolicy,
    ) -> Result<(), EngineError> {
        reject_paused_type(data_type)?;
        let field = self
            .lookup_field(column)
            .ok_or(EngineError::UnknownColumn(column))?;
        let old_type = field.data_type.clone();
        reject_paused_cast(&old_type, data_type)?;
        // Cumulative text budget: only timestamp timezone text can change
        // (names, counts and metadata are untouched by a cast). The legacy
        // full rebuild catches the crossing inside LogicalSchema::new and
        // maps it to the cast message; replicate at the same rule.
        let new_total = self
            .text_bytes
            .checked_sub(type_timezone_text(&old_type))
            .and_then(|base| base.checked_add(type_timezone_text(data_type)))
            .unwrap_or(MAX_SCHEMA_TEXT_BYTES + 1);
        if new_total > MAX_SCHEMA_TEXT_BYTES {
            return Err(EngineError::InvalidPlan("cast produced an invalid schema"));
        }
        let field = self
            .schema
            .fields
            .iter_mut()
            .find(|field| field.id == column)
            .ok_or(EngineError::UnknownColumn(column))?;
        field.data_type = data_type.clone();
        if matches!(on_failure, CastFailurePolicy::SetNull) {
            field.nullable = true;
        }
        self.text_bytes = new_total;
        // Option 3 (#166): cast changes type/nullability only, never the
        // ColumnId -> ordinal mapping.
        Ok(())
    }

    fn apply_derive(
        &mut self,
        id: stillflow_core::ColumnId,
        name: &str,
        data_type: &LogicalType,
        nullable: bool,
        expression: &stillflow_core::Expr,
    ) -> Result<(), EngineError> {
        validate_expr(expression, &*self)?;
        let inferred = type_check_expr_in(expression, &*self)?;
        reject_paused_type(data_type)?;
        if !matches!(inferred, LogicalType::Null) && inferred != *data_type {
            return Err(EngineError::TypeError(
                "derived column type does not match the typed expression",
            ));
        }
        if self.schema.field(id).is_some()
            || self.schema.fields.iter().any(|field| field.name == name)
        {
            return Err(EngineError::InvalidPlan(
                "derived column id or name is not unique",
            ));
        }
        reject_paused_cast_in_expr(expression, &*self)?;
        let nullable_inferred = infer_nullability(expression, &*self)?;
        if !nullable && nullable_inferred {
            return Err(EngineError::TypeError(
                "derived column nullability is narrower than the expression",
            ));
        }
        // Legacy order after the local checks: LogicalField::new (field-level
        // validity, message "derived field is invalid"), then the schema
        // rebuild which rejects field-count and cumulative-text crossings
        // with "derive produced an invalid schema" — replicate both at this
        // exact position.
        let field = LogicalField::new(id, name.to_owned(), data_type.clone(), nullable)
            .map_err(|_| EngineError::InvalidPlan("derived field is invalid"))?;
        if self.schema.fields.len() >= MAX_SCHEMA_FIELDS {
            return Err(EngineError::InvalidPlan(
                "derive produced an invalid schema",
            ));
        }
        let added_text = field_text(&field) + type_timezone_text(data_type);
        let new_total = self
            .text_bytes
            .checked_add(added_text)
            .unwrap_or(MAX_SCHEMA_TEXT_BYTES + 1);
        if new_total > MAX_SCHEMA_TEXT_BYTES {
            return Err(EngineError::InvalidPlan(
                "derive produced an invalid schema",
            ));
        }
        self.schema.fields.push(field);
        self.text_bytes = new_total;
        self.refresh();
        Ok(())
    }

    /// ApplyRules-boundary conversion. Runs the FULL `LogicalSchema::new`
    /// validation in the production path — the permanent safety oracle —
    /// and yields the canonical schema for downstream steps.
    pub(crate) fn into_schema(self) -> Result<LogicalSchema, EngineError> {
        LogicalSchema::new(self.schema.fields)
            .map_err(|_| EngineError::InvalidPlan("apply-rules produced an invalid schema"))
    }

    /// Budget-accounting exactness probe (used by tests and by the
    /// differential harness): the maintained counter must equal a fresh
    /// core-style recount of the current field state.
    #[cfg(test)]
    pub(crate) fn text_bytes_exact(&self) -> bool {
        self.text_bytes == text_bytes_of(&self.schema)
    }
}

impl ColumnLookup for IncrementalSchema {
    fn lookup_field(&self, id: stillflow_core::ColumnId) -> Option<&LogicalField> {
        match &self.entries {
            Some(entries) => entries
                .binary_search_by_key(&id, |(key, _)| *key)
                .ok()
                .and_then(|position| {
                    let ordinal = entries[position].1 as usize;
                    self.schema.fields.get(ordinal)
                }),
            None => self.schema.field(id),
        }
    }
}

impl ColumnLookup for &IncrementalSchema {
    fn lookup_field(&self, id: stillflow_core::ColumnId) -> Option<&LogicalField> {
        (*self).lookup_field(id)
    }
}

#[cfg(test)]
mod tests {
    //! O0-B2-A2 production correctness oracle (#159): the legacy per-rule
    //! full-rebuild path and the production incremental layer must agree
    //! exactly — same Ok payload (serialized form + canonical bytes), or the
    //! same error variant/message at the same rule position — across every
    //! schema-mutating rule surface, valid and invalid, including the #145
    //! budget-boundary cases, plus a fixed-seed property battery. Failures
    //! print seed/case/rule.

    use super::*;
    use stillflow_core::{ColumnId, Expr, LogicalField};
    use stillflow_plan::CastFailurePolicy;
    use uuid::Uuid;

    fn id(n: u128) -> ColumnId {
        ColumnId::from_uuid(Uuid::from_u128(n))
    }

    /// Mixed-type field list with unique ids/names.
    fn fields(count: usize) -> Vec<LogicalField> {
        (0..count)
            .map(|i| {
                LogicalField::new(
                    id(i as u128 + 1),
                    format!("c{i}"),
                    if i % 4 == 3 {
                        LogicalType::Int64
                    } else {
                        LogicalType::Utf8
                    },
                    i % 2 == 0,
                )
                .expect("test field is valid")
            })
            .collect()
    }

    /// Schema whose cumulative text bytes equal `total` exactly, with unique
    /// ids/names of the requested count (names padded to reach the total).
    fn budget_schema(count: usize, total: usize) -> LogicalSchema {
        assert!(count > 0 && total >= count * 7);
        let base = 6; // "c{i:04}" + one distinguishing char
        let mut fields = Vec::with_capacity(count);
        let mut used = 0_usize;
        for i in 0..count {
            let head = format!("c{i:04}x"); // 6 bytes
            let padding = if i == count - 1 {
                total - used - base
            } else {
                (total - used) / (count - i) - base
            };
            let name = format!("{head}{}", "y".repeat(padding));
            assert_eq!(name.len(), base + padding);
            let name_len = name.len();
            fields.push(
                LogicalField::new(id(i as u128 + 9000), name, LogicalType::Int64, false)
                    .expect("budget field"),
            );
            used += name_len;
        }
        assert_eq!(used, total);
        LogicalSchema::new(fields).expect("budget schema is valid")
    }

    fn pred(left: u128, right: u128) -> Expr {
        Expr::Binary {
            left: Box::new(Expr::Column(id(left))),
            operator: stillflow_core::BinaryOperator::GreaterThan,
            right: Box::new(Expr::Column(id(right))),
        }
    }

    fn derive(id_value: u128, name: &str, ty: LogicalType, expr: Expr) -> Rule {
        Rule::DeriveColumn {
            id: id(id_value),
            name: name.to_owned(),
            data_type: ty,
            nullable: false,
            expression: expr,
        }
    }

    /// Runs one chain through BOTH paths from a shared base. Panics with the
    /// rule position on any divergence (Ok/Err disagreement, error message
    /// drift, boundary-oracle firing where legacy passed, schema divergence,
    /// or budget-counter drift).
    fn assert_chain_equivalent(base: LogicalSchema, rules: &[Rule]) {
        let mut legacy = Some(base.clone());
        let mut layer = IncrementalSchema::from_schema(base);
        if layer.text_bytes != text_bytes_of(&layer.schema) {
            panic!("counter drift at construction");
        }
        for (position, rule) in rules.iter().enumerate() {
            let legacy_step = match legacy.take() {
                Some(schema) => crate::preflight::apply_rule_schema_legacy(schema, rule).map(Some),
                None => unreachable!("legacy chain terminated without divergence"),
            };
            let incremental_step = layer.apply_rule(rule);
            match (&legacy_step, &incremental_step) {
                (Ok(_), Ok(())) => {}
                (Err(legacy_error), Err(incremental_error)) => {
                    assert_eq!(
                        format!("{legacy_error:?}"),
                        format!("{incremental_error:?}"),
                        "rule {position} ({rule:?}): error variant/message diverged"
                    );
                    // Both failed: chains terminate identically at position.
                    return;
                }
                _ => panic!(
                    "rule {position} ({rule:?}): Ok/Err disagreement                      legacy={legacy_step:?} incremental={incremental_step:?}"
                ),
            }
            assert!(
                layer.text_bytes_exact(),
                "counter drift after rule {position}"
            );
            legacy = legacy_step.expect("checked above");
        }
        // The permanent boundary oracle must never fire where legacy passed.
        let boundary = layer.into_schema();
        assert!(
            boundary.is_ok(),
            "boundary safety oracle fired on a chain legacy accepted: {boundary:?}"
        );
        let legacy_schema = legacy.expect("chain survived");
        let boundary_schema = boundary.expect("checked above");
        assert_eq!(
            serde_json::to_string(&legacy_schema).expect("serialize"),
            serde_json::to_string(&boundary_schema).expect("serialize"),
            "serialized schema diverged"
        );
        assert_eq!(
            legacy_schema.canonical_bytes().expect("canonical"),
            boundary_schema.canonical_bytes().expect("canonical"),
            "canonical bytes diverged"
        );
    }

    #[test]
    fn rename_paths_agree() {
        let rules = vec![
            Rule::Rename {
                column: id(1),
                to: "renamed".into(),
            },
            Rule::Rename {
                column: id(99),
                to: "ghost".into(),
            },
            Rule::Rename {
                column: id(2),
                to: "".into(),
            },
            Rule::Rename {
                column: id(2),
                to: "   ".into(),
            },
            Rule::Rename {
                column: id(2),
                to: "c0".into(),
            },
            Rule::Rename {
                column: id(1),
                to: "c1".into(),
            },
        ];
        for take in 1..=rules.len() {
            assert_chain_equivalent(
                LogicalSchema::new(fields(8)).expect("schema"),
                &rules[..take],
            );
        }
    }

    #[test]
    fn drop_paths_agree() {
        let rules = vec![
            Rule::DropColumn { column: id(1) },
            Rule::DropColumn { column: id(99) },
            Rule::DropColumn { column: id(1) },
        ];
        assert_chain_equivalent(LogicalSchema::new(fields(2)).expect("schema"), &rules);
        assert_chain_equivalent(LogicalSchema::new(fields(64)).expect("schema"), &rules[..1]);
    }

    #[test]
    fn trim_cast_replace_fillnull_paths_agree() {
        let rules = vec![
            Rule::Trim { column: id(1) },
            Rule::Trim { column: id(4) },
            Rule::Trim { column: id(99) },
            Rule::Cast {
                column: id(1),
                data_type: LogicalType::Int64,
                on_failure: CastFailurePolicy::SetNull,
            },
            Rule::Cast {
                column: id(1),
                data_type: LogicalType::Binary,
                on_failure: CastFailurePolicy::Error,
            },
            Rule::Cast {
                column: id(99),
                data_type: LogicalType::Int64,
                on_failure: CastFailurePolicy::Error,
            },
            Rule::ReplaceLiteral {
                column: id(1),
                from: ScalarValue::Utf8("a".into()),
                to: ScalarValue::Null,
            },
            Rule::ReplaceLiteral {
                column: id(1),
                from: ScalarValue::Int64(1),
                to: ScalarValue::Null,
            },
            Rule::ReplaceLiteral {
                column: id(99),
                from: ScalarValue::Null,
                to: ScalarValue::Null,
            },
            Rule::FillNull {
                column: id(1),
                value: ScalarValue::Utf8("x".into()),
            },
            Rule::FillNull {
                column: id(1),
                value: ScalarValue::Null,
            },
            Rule::FillNull {
                column: id(99),
                value: ScalarValue::Int64(7),
            },
        ];
        for take in 1..=rules.len() {
            assert_chain_equivalent(
                LogicalSchema::new(fields(8)).expect("schema"),
                &rules[..take],
            );
        }
    }

    #[test]
    fn derive_paths_agree() {
        let cast_expr = |col: u128| Expr::Cast {
            expression: Box::new(Expr::Column(id(col))),
            data_type: LogicalType::Int64,
        };
        let rules = vec![
            derive(500, "d0", LogicalType::Int64, cast_expr(4)),
            derive(500, "d1", LogicalType::Int64, cast_expr(8)), // duplicate id
            derive(501, "c0", LogicalType::Int64, cast_expr(4)), // duplicate name
            derive(501, "d2", LogicalType::Utf8, cast_expr(4)),  // type mismatch
            derive(501, "", LogicalType::Int64, cast_expr(4)),   // empty name
            derive(501, "d3", LogicalType::Int64, cast_expr(99)), // unknown column
            derive(
                501,
                "d4",
                LogicalType::Boolean,
                Expr::Literal(ScalarValue::Boolean(true)),
            ),
        ];
        for take in 1..=rules.len() {
            assert_chain_equivalent(
                LogicalSchema::new(fields(8)).expect("schema"),
                &rules[..take],
            );
        }
    }

    #[test]
    fn mixed_chains_agree() {
        let rules = vec![
            Rule::Rename {
                column: id(1),
                to: "renamed_a".into(),
            },
            Rule::Cast {
                column: id(4),
                data_type: LogicalType::Float64,
                on_failure: CastFailurePolicy::SetNull,
            },
            Rule::ReplaceLiteral {
                column: id(1),
                from: ScalarValue::Utf8("x".into()),
                to: ScalarValue::Null,
            },
            Rule::FillNull {
                column: id(4),
                value: ScalarValue::Float64(stillflow_core::FiniteF64::new(1.5).expect("finite")),
            },
            Rule::FilterRows {
                predicate: pred(1, 4),
            },
            Rule::FilterRows {
                predicate: Expr::Literal(ScalarValue::Int64(1)),
            },
            Rule::DeriveColumn {
                id: id(600),
                name: "total".into(),
                data_type: LogicalType::Int64,
                nullable: false,
                expression: Expr::Cast {
                    expression: Box::new(Expr::Column(id(4))),
                    data_type: LogicalType::Int64,
                },
            },
            Rule::Trim { column: id(1) }, // int column after cast -> type error
        ];
        for take in 1..=rules.len() {
            assert_chain_equivalent(
                LogicalSchema::new(fields(8)).expect("schema"),
                &rules[..take],
            );
        }
    }

    #[test]
    fn wide_chain_agrees_f64() {
        let rules: Vec<Rule> = (0..16)
            .map(|i| Rule::Rename {
                column: id(i as u128 % 8 + 1),
                to: format!("w{i}"),
            })
            .chain((0..8).map(|i| Rule::DropColumn {
                column: id(i as u128 % 8 + 1),
            }))
            .collect();
        assert_chain_equivalent(LogicalSchema::new(fields(64)).expect("schema"), &rules[..4]);
        assert_chain_equivalent(LogicalSchema::new(fields(64)).expect("schema"), &rules);
    }

    // ------------------------------------------------------------------
    // #145 closure: budget-failure fidelity (Finding 1)
    // ------------------------------------------------------------------

    #[test]
    fn rename_text_budget_crossing_fails_at_same_rule() {
        let near_max = MAX_SCHEMA_TEXT_BYTES - 10;
        let schema = budget_schema(300, near_max);
        // The renamed field's name is ~3.4KB; a target LONGER than it by more
        // than the 10-byte slack crosses the cumulative text budget.
        let old_len = schema.field(id(9000)).expect("field").name.len();
        let longer = "y".repeat(old_len + 11);
        let rules = vec![
            Rule::Rename {
                column: id(9000),
                to: longer.clone(),
            }, // crosses
            Rule::Rename {
                column: id(9001),
                to: "must_not_run".into(),
            },
        ];
        assert_chain_equivalent(schema, &rules);
        // Exact pin: legacy error at rule 0 == UnknownColumn(9000).
        let direct = crate::preflight::apply_rule_schema_legacy(
            budget_schema(300, near_max),
            &Rule::Rename {
                column: id(9000),
                to: longer,
            },
        );
        let err = direct.expect_err("budget crossing must fail in legacy");
        assert!(format!("{err:?}").contains("UnknownColumn"), "{err:?}");
    }

    #[test]
    fn derive_field_count_budget_crossing_fails_at_same_rule() {
        let full = LogicalSchema::new(fields(MAX_SCHEMA_FIELDS)).expect("max schema");
        let rules = vec![derive(
            999_000,
            "overflow",
            LogicalType::Int64,
            Expr::Cast {
                expression: Box::new(Expr::Column(id(1))),
                data_type: LogicalType::Int64,
            },
        )];
        assert_chain_equivalent(full, &rules);
    }

    #[test]
    fn derive_text_budget_crossing_fails_at_same_rule() {
        let near_max = MAX_SCHEMA_TEXT_BYTES - 10;
        let schema = budget_schema(300, near_max);
        let rules = vec![
            derive(
                999_001,
                "plus_twenty_five_bytes_long_n",
                LogicalType::Int64,
                Expr::Cast {
                    expression: Box::new(Expr::Column(id(9000))),
                    data_type: LogicalType::Int64,
                },
            ),
            Rule::Rename {
                column: id(9001),
                to: "must_not_run".into(),
            },
        ];
        assert_chain_equivalent(schema, &rules);
    }

    #[test]
    fn cast_timezone_text_budget_crossing_fails_at_same_rule() {
        let near_max = MAX_SCHEMA_TEXT_BYTES - 10;
        let schema = budget_schema(300, near_max); // all Int64, no timezone text
        let tz = "T".repeat(30);
        let rules = vec![
            Rule::Cast {
                column: id(9000),
                data_type: LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some(tz),
                },
                on_failure: CastFailurePolicy::Error,
            },
            Rule::Rename {
                column: id(9001),
                to: "must_not_run".into(),
            },
        ];
        assert_chain_equivalent(schema, &rules);
    }

    #[test]
    fn near_budget_valid_operations_do_not_false_positive() {
        // Rename/drop/derive that KEEP the total under the budget must pass
        // through both paths identically (no false budget fires).
        let near_max = MAX_SCHEMA_TEXT_BYTES - 10_000;
        let schema = budget_schema(300, near_max);
        let shrink = vec![
            Rule::Rename {
                column: id(9000),
                to: "xy".into(),
            }, // shrinks text
            Rule::DropColumn { column: id(9001) }, // shrinks text
        ];
        assert_chain_equivalent(schema, &shrink);
        // Growth by less than the remaining slack must pass (no false fire).
        let schema2 = budget_schema(300, near_max);
        let old_len2 = schema2.field(id(9000)).expect("field").name.len();
        let grow_but_ok = vec![
            Rule::Rename {
                column: id(9000),
                to: "x".repeat(old_len2 + 4),
            }, // +4 < slack
        ];
        assert_chain_equivalent(schema2, &grow_but_ok);
    }

    // ------------------------------------------------------------------
    // Fixed-seed property battery (anti-drift, #159 item 2)
    // ------------------------------------------------------------------

    #[test]
    fn property_battery_legacy_equals_incremental() {
        let seed = 0x000A_25EE_D159_u64; // fixed seed (A2)
        let mut state = seed;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for case in 0..400 {
            let f = (next() % 24 + 1) as usize;
            let mut rules = Vec::new();
            let rule_count = (next() % 8 + 1) as usize;
            for r in 0..rule_count {
                let column = (next() % (f as u64) + 1) as u128;
                let other = (next() % (f as u64) + 1) as u128;
                match next() % 9 {
                    0 => rules.push(Rule::Rename {
                        column: id(column),
                        to: format!("r{case}_{r}"),
                    }),
                    1 => rules.push(Rule::Rename {
                        column: id(900_000 + (next() % 9) as u128),
                        to: "ghost".into(),
                    }),
                    2 => rules.push(Rule::Rename {
                        column: id(column),
                        to: if next() % 2 == 0 {
                            "".into()
                        } else {
                            "   ".into()
                        },
                    }),
                    3 => rules.push(Rule::Rename {
                        column: id(999),
                        to: "c0".into(),
                    }),
                    4 => rules.push(Rule::DropColumn { column: id(column) }),
                    5 => rules.push(Rule::DropColumn { column: id(999) }),
                    6 => rules.push(Rule::Cast {
                        column: id(column),
                        data_type: LogicalType::Int64,
                        on_failure: CastFailurePolicy::SetNull,
                    }),
                    7 => rules.push(Rule::FillNull {
                        column: id(column),
                        value: ScalarValue::Int64(7),
                    }),
                    _ => rules.push(Rule::ReplaceLiteral {
                        column: id(column),
                        from: ScalarValue::Null,
                        to: ScalarValue::Null,
                    }),
                }
                let _ = other;
            }
            if let Err(message) = std::panic::catch_unwind(|| {
                assert_chain_equivalent(LogicalSchema::new(fields(f)).expect("base"), &rules);
            }) {
                let payload = message
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| message.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "unknown panic".to_owned());
                panic!(
                    "property battery divergence — seed={seed:#x} case={case} f={f} rules={rule_count}                      chain={rules:?}
{payload}"
                );
            }
        }
    }

    // ------------------------------------------------------------------
    // O0-B2-A1-A2 (#163): the A1 ordinal index inside the A2 mutation layer
    // must be exactly equivalent to the linear reference on every surface,
    // and the index must NEVER be stale after any accepted or rejected rule
    // (structural guarantee mechanically enforced here per rule).
    // ------------------------------------------------------------------

    /// Per-rule differential with the A1 ordinal index forced on (indexed
    /// backend), plus mechanical `verify_entries` + counter-exactness after
    /// every rule and boundary-oracle parity at the end.
    fn assert_indexed_chain_equivalent(base: LogicalSchema, rules: &[Rule]) {
        let mut linear = Some(base.clone());
        let mut indexed = IncrementalSchema::indexed(base);
        assert!(indexed.verify_entries(), "index stale at construction");
        for (position, rule) in rules.iter().enumerate() {
            let linear_step = match linear.take() {
                Some(schema) => crate::preflight::apply_rule_schema_legacy(schema, rule).map(Some),
                None => unreachable!("linear chain terminated without divergence"),
            };
            let indexed_step = indexed.apply_rule(rule);
            match (&linear_step, &indexed_step) {
                (Ok(_), Ok(())) => {}
                (Err(le), Err(ie)) => {
                    assert_eq!(
                        format!("{le:?}"),
                        format!("{ie:?}"),
                        "rule {position} ({rule:?}): indexed/linear error diverged"
                    );
                    assert!(
                        indexed.verify_entries(),
                        "STALE INDEX after rejected rule {position}"
                    );
                    assert!(
                        indexed.text_bytes_exact(),
                        "counter drift after rejected rule {position}"
                    );
                    return;
                }
                _ => panic!(
                    "rule {position} ({rule:?}): indexed/linear Ok/Err disagreement                        linear={linear_step:?} indexed={indexed_step:?}"
                ),
            }
            assert!(
                indexed.verify_entries(),
                "STALE INDEX after accepted rule {position}"
            );
            assert!(
                indexed.text_bytes_exact(),
                "counter drift after accepted rule {position}"
            );
            assert_eq!(
                indexed.ordinal_of(id((position as u128 % 8) + 1)),
                indexed
                    .schema
                    .fields
                    .iter()
                    .position(|f| f.id == id((position as u128 % 8) + 1)),
                "ordinal_of diverges from linear position after rule {position}"
            );
            linear = linear_step.expect("checked above");
        }
        // The permanent boundary oracle must never fire where legacy passed.
        let boundary = indexed.into_schema();
        assert!(
            boundary.is_ok(),
            "boundary safety oracle fired on an indexed chain legacy accepted: {boundary:?}"
        );
        let linear_schema = linear.expect("chain survived");
        let boundary_schema = boundary.expect("checked above");
        assert_eq!(
            serde_json::to_string(&linear_schema).expect("serialize"),
            serde_json::to_string(&boundary_schema).expect("serialize"),
            "serialized schema diverged (indexed)"
        );
        assert_eq!(
            linear_schema.canonical_bytes().expect("canonical"),
            boundary_schema.canonical_bytes().expect("canonical"),
            "canonical bytes diverged (indexed)"
        );
    }

    #[test]
    fn indexed_matches_linear_on_every_rule_surface() {
        let cast_expr = |col: u128| Expr::Cast {
            expression: Box::new(Expr::Column(id(col))),
            data_type: LogicalType::Int64,
        };
        let chains: Vec<Vec<Rule>> = vec![
            vec![
                Rule::Rename {
                    column: id(1),
                    to: "renamed".into(),
                },
                Rule::Rename {
                    column: id(99),
                    to: "ghost".into(),
                },
                Rule::Rename {
                    column: id(2),
                    to: "".into(),
                },
                Rule::Rename {
                    column: id(2),
                    to: "c0".into(),
                },
            ],
            vec![
                Rule::DropColumn { column: id(1) },
                Rule::DropColumn { column: id(99) },
                Rule::DropColumn { column: id(3) },
            ],
            vec![
                Rule::Trim { column: id(4) },
                Rule::Trim { column: id(99) },
                Rule::Cast {
                    column: id(1),
                    data_type: LogicalType::Int64,
                    on_failure: CastFailurePolicy::SetNull,
                },
                Rule::Cast {
                    column: id(1),
                    data_type: LogicalType::Binary,
                    on_failure: CastFailurePolicy::Error,
                },
                Rule::ReplaceLiteral {
                    column: id(1),
                    from: ScalarValue::Utf8("a".into()),
                    to: ScalarValue::Null,
                },
                Rule::ReplaceLiteral {
                    column: id(99),
                    from: ScalarValue::Null,
                    to: ScalarValue::Null,
                },
                Rule::FillNull {
                    column: id(1),
                    value: ScalarValue::Utf8("x".into()),
                },
                Rule::FillNull {
                    column: id(1),
                    value: ScalarValue::Null,
                },
            ],
            vec![
                derive(500, "d0", LogicalType::Int64, cast_expr(4)),
                derive(500, "d1", LogicalType::Int64, cast_expr(8)),
                derive(501, "c0", LogicalType::Int64, cast_expr(4)),
                derive(501, "", LogicalType::Int64, cast_expr(4)),
                derive(501, "d3", LogicalType::Int64, cast_expr(99)),
            ],
            vec![
                Rule::Rename {
                    column: id(1),
                    to: "renamed_a".into(),
                },
                Rule::Cast {
                    column: id(4),
                    data_type: LogicalType::Float64,
                    on_failure: CastFailurePolicy::SetNull,
                },
                Rule::FillNull {
                    column: id(4),
                    value: ScalarValue::Float64(
                        stillflow_core::FiniteF64::new(1.5).expect("finite"),
                    ),
                },
                Rule::FilterRows {
                    predicate: pred(1, 4),
                },
                Rule::FilterRows {
                    predicate: Expr::Literal(ScalarValue::Int64(1)),
                },
                derive(600, "total", LogicalType::Int64, cast_expr(4)),
                Rule::Trim { column: id(1) },
            ],
            vec![
                Rule::Validate {
                    predicate: Expr::Literal(ScalarValue::Boolean(true)),
                    severity: stillflow_plan::ValidationSeverity::Error,
                    message: "m".into(),
                },
                Rule::Deduplicate { keys: vec![id(1)] },
            ],
        ];
        for chain in chains {
            for take in 1..=chain.len() {
                assert_indexed_chain_equivalent(
                    LogicalSchema::new(fields(8)).expect("base"),
                    &chain[..take],
                );
            }
        }
    }

    #[test]
    fn indexed_mutation_sequences_never_stale_with_ordinal_shifts() {
        // A scripted sequence where every structural mutation class occurs
        // after earlier renames: drop shifts ordinals of later fields, derive
        // appends, cast/replace/fill leave ids/positions untouched. The
        // indexed chain must agree with legacy per-rule and the index must
        // stay the deterministic image of the field list throughout.
        let mut ruler: Vec<Rule> = Vec::new();
        for i in 0..16u128 {
            ruler.push(Rule::Rename {
                column: id(i % 8 + 1),
                to: format!("step{i}"),
            });
        }
        ruler.push(Rule::DropColumn { column: id(2) });
        ruler.push(Rule::DropColumn { column: id(4) });
        ruler.push(Rule::Cast {
            column: id(1),
            data_type: LogicalType::Float64,
            on_failure: CastFailurePolicy::SetNull,
        });
        ruler.push(Rule::ReplaceLiteral {
            column: id(1),
            from: ScalarValue::Null,
            to: ScalarValue::Null,
        });
        ruler.push(Rule::FillNull {
            column: id(6),
            value: ScalarValue::Int64(0),
        });
        ruler.push(derive(
            700,
            "tail",
            LogicalType::Int64,
            Expr::Literal(ScalarValue::Int64(1)),
        ));
        ruler.push(Rule::DropColumn { column: id(8) });
        ruler.push(derive(
            701,
            "tail2",
            LogicalType::Int64,
            Expr::Literal(ScalarValue::Int64(2)),
        ));
        for take in 1..=ruler.len() {
            assert_indexed_chain_equivalent(
                LogicalSchema::new(fields(8)).expect("base"),
                &ruler[..take],
            );
        }
    }

    #[test]
    fn indexed_budget_edges_parity() {
        // The A1A2 index must not alter A2 exact budget-fidelity: rerun the
        // frozen budget-edge constructions through the indexed backend.
        let near_max = MAX_SCHEMA_TEXT_BYTES - 10;
        let schema = budget_schema(300, near_max);
        let old_len = schema.field(id(9000)).expect("field").name.len();
        assert_indexed_chain_equivalent(
            schema,
            &[
                Rule::Rename {
                    column: id(9000),
                    to: "y".repeat(old_len + 11),
                },
                Rule::Rename {
                    column: id(9001),
                    to: "must_not_run".into(),
                },
            ],
        );
        let full = LogicalSchema::new(fields(MAX_SCHEMA_FIELDS)).expect("max schema");
        assert_indexed_chain_equivalent(
            full,
            &[derive(
                999_000,
                "overflow",
                LogicalType::Int64,
                Expr::Cast {
                    expression: Box::new(Expr::Column(id(1))),
                    data_type: LogicalType::Int64,
                },
            )],
        );
        let shrink = vec![
            Rule::Rename {
                column: id(9000),
                to: "xy".into(),
            },
            Rule::DropColumn { column: id(9001) },
        ];
        assert_indexed_chain_equivalent(budget_schema(300, near_max), &shrink);
    }

    #[test]
    fn indexed_policy_and_swap_preserve_determinism() {
        // for_shape routes by the deterministic A1 policy; swap_with keeps
        // the decision; lookups agree with linear on every position.
        let wide = LogicalSchema::new(fields(512)).expect("wide");
        let small = LogicalSchema::new(fields(16)).expect("small");
        let indexed = IncrementalSchema::for_shape(wide.clone(), 512); // >= max(32, 64)
        let not_indexed = IncrementalSchema::for_shape(wide, 8); // < 64
        let small_indexed = IncrementalSchema::for_shape(small.clone(), 32); // >= max(32, 2)
        assert!(
            indexed.entries.is_some(),
            "policy must index wide/high-lookup"
        );
        assert!(
            not_indexed.entries.is_none(),
            "policy must stay linear low-lookup"
        );
        assert!(small_indexed.entries.is_some(), "policy F/8 floor at 32");
        assert!(
            IncrementalSchema::for_shape(small, 31).entries.is_none(),
            "policy below 32 stays linear"
        );
        for f in [64usize, 512, 2048, 4096] {
            let schema = LogicalSchema::new(fields(f)).expect("schema");
            assert_eq!(
                IncrementalSchema::for_shape(schema.clone(), f)
                    .entries
                    .is_some(),
                crate::lookup::use_index(schema.fields.len(), f),
                "for_shape deviates from use_index at F={f}"
            );
            let idx = IncrementalSchema::indexed(schema.clone());
            let swapped = idx.swap_with(schema.clone());
            assert!(swapped.entries.is_some(), "swap_with must preserve indexed");
            assert!(swapped.verify_entries());
        }
    }

    // ------------------------------------------------------------------
    // O0-B2-A1-A2-FINAL-INTEGRATION (#166) Option-3 mechanical tests
    // ------------------------------------------------------------------

    #[test]
    fn option3_ordinal_preserving_rules_leave_entries_bit_exact() {
        // Rename / Cast / ReplaceLiteral / FillNull must not change the
        // ColumnId -> ordinal table at all (before == after), and the
        // retained entries must equal a fresh rebuild of the post-rule
        // schema (no stale drift possible).
        let before = build_ordinals(&LogicalSchema::new(fields(64)).expect("base"));
        let mut working = IncrementalSchema::indexed(LogicalSchema::new(fields(64)).expect("base"));
        working
            .apply_rule(&Rule::Rename {
                column: id(1),
                to: "renamed".into(),
            })
            .expect("rename");
        assert!(working.verify_entries());
        working
            .apply_rule(&Rule::Cast {
                column: id(2),
                data_type: LogicalType::Float64,
                on_failure: CastFailurePolicy::Error,
            })
            .expect("cast");
        assert!(working.verify_entries());
        working
            .apply_rule(&Rule::ReplaceLiteral {
                column: id(3),
                from: ScalarValue::Null,
                to: ScalarValue::Null,
            })
            .expect("replace");
        assert!(working.verify_entries());
        working
            .apply_rule(&Rule::FillNull {
                column: id(4),
                value: ScalarValue::Int64(0),
            })
            .expect("fillnull");
        assert!(working.verify_entries());
        assert!(
            working.entries.as_ref().expect("indexed") == &before,
            "ordinal-preserving rules must leave entries bit-exactly unchanged"
        );
        assert_eq!(
            working.lookup_field(id(1)).map(|f| f.name.as_str()),
            Some("renamed"),
            "indexed lookup resolves the renamed field"
        );
    }

    #[test]
    fn option3_drop_shifts_and_derive_appends_with_exact_rebuild() {
        let mut working = IncrementalSchema::indexed(LogicalSchema::new(fields(8)).expect("base"));
        working
            .apply_rule(&Rule::Rename {
                column: id(5),
                to: "r5".into(),
            })
            .expect("rename");
        let ordinal = |w: &IncrementalSchema, c: u128| -> usize {
            w.entries
                .as_ref()
                .expect("indexed")
                .iter()
                .find(|(k, _)| *k == id(c))
                .expect("present")
                .1 as usize
        };
        assert_eq!(ordinal(&working, 4), 3);
        working
            .apply_rule(&Rule::DropColumn { column: id(3) })
            .expect("drop");
        assert_eq!(ordinal(&working, 4), 2, "drop must shift later ordinals");
        assert_eq!(
            working.entries.as_ref().expect("indexed"),
            &build_ordinals(&working.schema)
        );
        assert!(working.verify_entries());
        working
            .apply_rule(&derive(
                900,
                "new_field",
                LogicalType::Int64,
                Expr::Literal(ScalarValue::Int64(1)),
            ))
            .expect("derive");
        assert_eq!(
            ordinal(&working, 900),
            working.schema.fields.len() - 1,
            "derived field at the new last ordinal"
        );
        assert_eq!(
            working.entries.as_ref().expect("indexed"),
            &build_ordinals(&working.schema)
        );
        assert!(working.verify_entries());
        assert!(working.text_bytes_exact());
    }

    #[test]
    fn option3_failed_mutations_keep_schema_and_entries_consistent() {
        let bad_derive = derive(
            901,
            "bad",
            LogicalType::Utf8,
            Expr::Literal(ScalarValue::Int64(1)),
        );
        assert_indexed_chain_equivalent(
            LogicalSchema::new(fields(8)).expect("base"),
            &[
                bad_derive.clone(),
                Rule::Rename {
                    column: id(1),
                    to: "".into(),
                },
                Rule::Rename {
                    column: id(2),
                    to: "ok".into(),
                },
            ],
        );
        let mut working = IncrementalSchema::indexed(LogicalSchema::new(fields(8)).expect("base"));
        assert!(working.apply_rule(&bad_derive).is_err());
        assert_eq!(
            working.entries.as_ref().expect("indexed"),
            &build_ordinals(&working.schema)
        );
        assert!(working.verify_entries());
        assert_indexed_chain_equivalent(
            LogicalSchema::new(fields(8)).expect("base"),
            &[
                Rule::Rename {
                    column: id(1),
                    to: "ok".into(),
                },
                Rule::DropColumn { column: id(2) },
            ],
        );
    }

    #[test]
    fn option3_mixed_sequence_verifies_after_every_rule() {
        // Rename -> Cast(SetNull) -> FillNull -> Drop -> ReplaceLiteral ->
        // Derive: indexed chain must agree with legacy per rule and
        // verify_entries must hold after every rule (drop/derive rebuild,
        // the rest keep entries exact).
        let rules = vec![
            Rule::Rename {
                column: id(1),
                to: "a1".into(),
            },
            Rule::Cast {
                column: id(2),
                data_type: LogicalType::Float64,
                on_failure: CastFailurePolicy::SetNull,
            },
            Rule::FillNull {
                column: id(2),
                value: ScalarValue::Float64(stillflow_core::FiniteF64::new(1.5).expect("finite")),
            },
            Rule::DropColumn { column: id(3) },
            Rule::ReplaceLiteral {
                column: id(4),
                from: ScalarValue::Null,
                to: ScalarValue::Null,
            },
            derive(
                902,
                "derived_now",
                LogicalType::Int64,
                Expr::Literal(ScalarValue::Int64(2)),
            ),
        ];
        for take in 1..=rules.len() {
            assert_indexed_chain_equivalent(
                LogicalSchema::new(fields(8)).expect("base"),
                &rules[..take],
            );
        }
        // Sparse/non-indexed routing unchanged: policy below threshold stays
        // linear and behaves exactly like the plain A2 layer.
        let sparse = IncrementalSchema::for_shape(LogicalSchema::new(fields(8)).expect("base"), 1);
        assert!(sparse.entries.is_none(), "sparse policy stays linear");
        assert!(sparse.verify_entries());
    }
}
