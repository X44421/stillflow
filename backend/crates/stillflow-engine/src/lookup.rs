//! Engine-private `ColumnId -> ordinal` lookup index (O0-B2-A1 production).
//!
//! Expression/type resolution resolves columns through [`LogicalSchema::field`],
//! a linear scan over ordered fields. Wide schemas (F up to 4096) repeat that
//! scan for every referenced column, dominating preflight wall time. The
//! production path resolves columns through a deterministic private ordinal
//! index derived from the exact validated schema state; `LogicalSchema`
//! remains the sole authoritative public/core representation.
//!
//! Two invariants are enforced by construction:
//!
//! 1. **No stale index.** The indexed state lives in [`IndexedSchema`], a struct
//!    whose fields are private to this module, carried opaquely in the
//!    [`WorkingSchema::Indexed`] tuple variant — nothing outside `lookup.rs`
//!    can construct, destructure, or mutate the schema/index pair, so any
//!    schema change must flow through the mutation methods below. Every
//!    mutation (`rename` / `store` / `swap_with`) rebuilds the ordinal table
//!    from the exact new validated schema state *in the same method call*, so
//!    a stale table is unrepresentable: there is no code path that can change
//!    the schema without immediately replacing the index.
//!    [`WorkingSchema::verify`] additionally recomputes the table and compares
//!    it under `debug_assertions` after every mutation (supplemental
//!    mechanical guard; release correctness does not depend on it), and the
//!    test suite runs property-style consistency checks over every rule
//!    family.
//!
//! 2. **Deterministic construction policy.** The index is built only when
//!    [`use_index`] says the build is amortized. The decision is a pure
//!    function of plan/schema shape (field count and the exact number of
//!    lookups the preflight pass will serve), never of timing. Linear
//!    resolution stays the reference backend and is exercised byte-for-byte
//!    against the indexed backend by the in-crate differential suite, so the
//!    oracle survives productionization without the experiment feature split.

use stillflow_core::{ColumnId, LogicalField, LogicalSchema};

use crate::error::EngineError;

/// Abstraction over `ColumnId -> field` resolution used by the preflight
/// expression/type-resolution helpers.
///
/// The default backend is the authoritative [`LogicalSchema`] itself, whose
/// lookup is the unchanged linear scan over ordered fields. The indexed
/// backend is an Engine-private ordinal index derived from an already-validated
/// schema instance; both backends return the identical field for any valid
/// schema because validated schemas have unique column ids. This trait stays
/// private to the engine crate and never appears in public signatures.
pub(crate) trait ColumnLookup {
    fn lookup_field(&self, id: ColumnId) -> Option<&LogicalField>;
}

impl ColumnLookup for LogicalSchema {
    fn lookup_field(&self, id: ColumnId) -> Option<&LogicalField> {
        self.field(id)
    }
}

/// Deterministic shape-only index policy.
///
/// Linear resolution costs ~R·F/2 comparisons for R lookups over F fields;
/// indexed resolution costs ~F·log₂(F) to build plus R·log₂(F) to serve, so
/// the index pays when R ≳ 2·log₂(F). Wall-time measurements (experiment
/// #146, the independent rerun, and the production multi-window runs) show
/// the build's memory-access overhead is not amortized until R ≈ F/8, and
/// that at small F the build cost can exceed the scan savings even at the
/// F/8 ratio (F=64/R=24 measured ~ −5…−9%): the effective deterministic
/// threshold is therefore `max(32, F/8)`. It never routes the measured loss
/// regions (F=4096 sparse projection, F=64 small control) into the indexed
/// path, and it routes every measured win region (dense/near-full
/// projections, expression-heavy propagations, F=2048 bridge) into it.
pub(crate) const fn use_index(field_count: usize, served_lookups: usize) -> bool {
    let eighth_threshold = field_count / 8;
    let threshold = if eighth_threshold < 32 {
        32
    } else {
        eighth_threshold
    };
    served_lookups >= threshold
}

/// Deterministic `ColumnId -> ordinal` table for a validated schema: entries
/// are sorted by column id and ordinals refer to positions in `schema.fields`.
/// First occurrence wins so that resolution matches the linear scan even if an
/// unvalidated schema were ever supplied (validated schemas have unique ids,
/// so this never differs in practice).
fn build_ordinals(schema: &LogicalSchema) -> Vec<(ColumnId, u32)> {
    let mut entries = Vec::with_capacity(schema.fields.len());
    for (ordinal, field) in schema.fields.iter().enumerate() {
        let ordinal = u32::try_from(ordinal).expect("schema field count fits in u32 by validation");
        entries.push((field.id, ordinal));
    }
    // Stable sort keeps the first ordinal of equal ids, matching linear
    // first-match resolution; `dedup_by_key` then drops later duplicates.
    entries.sort_by_key(|(id, _)| *id);
    entries.dedup_by_key(|(id, _)| *id);
    entries
}

/// Borrowed view pairing a validated schema with its private ordinal index.
/// Used at the preflight "authorized schema" level, where the schema is
/// immutable and shared by the projection existence check and both scan
/// projections.
pub(crate) struct OrdinalIndex<'a> {
    schema: &'a LogicalSchema,
    entries: Vec<(ColumnId, u32)>,
}

impl<'a> OrdinalIndex<'a> {
    pub(crate) fn build(schema: &'a LogicalSchema) -> Self {
        Self {
            schema,
            entries: build_ordinals(schema),
        }
    }

    #[inline]
    pub(crate) fn field(&self, id: ColumnId) -> Option<&LogicalField> {
        self.ordinal(id)
            .and_then(|ordinal| self.schema.fields.get(ordinal))
    }

    #[inline]
    fn ordinal(&self, id: ColumnId) -> Option<usize> {
        self.entries
            .binary_search_by_key(&id, |(key, _)| *key)
            .ok()
            .map(|position| self.entries[position].1 as usize)
    }

    /// Recomputes the ordinal table from the current schema and compares it
    /// with the held entries; `false` means the index is inconsistent with its
    /// schema (unreachable by construction, mechanically guarded here).
    #[cfg(test)]
    pub(crate) fn verify(&self) -> bool {
        build_ordinals(self.schema) == self.entries
    }
}

/// Column resolution view over an immutable schema: the linear reference
/// backend or the shared Engine-private ordinal index, chosen by the
/// deterministic [`use_index`] policy.
pub(crate) enum AuthorizedLookup<'a> {
    Linear(&'a LogicalSchema),
    Indexed(OrdinalIndex<'a>),
}

impl<'a> AuthorizedLookup<'a> {
    /// Deterministic choice for a known schema shape and the exact number of
    /// lookups the calling pass will serve.
    pub(crate) fn for_shape(schema: &'a LogicalSchema, served_lookups: usize) -> Self {
        if use_index(schema.fields.len(), served_lookups) {
            Self::Indexed(OrdinalIndex::build(schema))
        } else {
            Self::Linear(schema)
        }
    }
}

impl ColumnLookup for AuthorizedLookup<'_> {
    fn lookup_field(&self, id: ColumnId) -> Option<&LogicalField> {
        match self {
            Self::Linear(schema) => schema.field(id),
            Self::Indexed(index) => index.field(id),
        }
    }
}

/// Indexed backend state: the exact validated schema plus its private ordinal
/// table. Both fields are **private to this module**; the opaque tuple variant
/// below means no code outside `lookup.rs` can construct, destructure, or
/// mutate this pair, so any schema change must flow through
/// [`WorkingSchema`]'s mutation methods, which rebuild the table from the
/// exact post-mutation state in the same call.
/// The struct name is crate-visible (it appears in the `pub(crate)` enum
/// variant's type) but both **fields are private to this module** — that is
/// what makes the schema/index pair opaque outside `lookup.rs`.
pub(crate) struct IndexedSchema {
    schema: LogicalSchema,
    entries: Vec<(ColumnId, u32)>,
}

/// Owned working schema threaded through rule-schema propagation.
///
/// The `Linear` variant is the unchanged production-baseline behavior (the
/// authoritative `LogicalSchema` and its linear scans) and doubles as the
/// byte-for-byte reference backend for the in-crate differential suite. The
/// `Indexed` variant keeps the exact same schema semantics and replaces only
/// where `ColumnId` resolution happens.
///
/// Structural stale-index guarantee: the indexed state is opaque (see
/// [`IndexedSchema`]), the schema is exposed only immutably (`schema()`), and
/// the three mutation methods (`rename` / `store` / `swap_with`) are the only
/// way the schema can change; each rebuilds the ordinal table from the exact
/// post-mutation validated schema state before returning, so no stale window
/// exists. Mutations replace or revalidate the whole field list through
/// [`LogicalSchema::new`] exactly like the baseline path (no incremental
/// validation semantics).
pub(crate) enum WorkingSchema {
    Linear(LogicalSchema),
    Indexed(IndexedSchema),
}

impl WorkingSchema {
    /// Deterministic choice for an owned schema and the exact number of
    /// lookups the propagation pass will serve.
    pub(crate) fn for_shape(schema: LogicalSchema, served_lookups: usize) -> Self {
        if use_index(schema.fields.len(), served_lookups) {
            Self::indexed(schema)
        } else {
            Self::Linear(schema)
        }
    }

    /// Reference backend: identical to the production baseline (linear scans
    /// over the authoritative `LogicalSchema`).
    #[cfg(test)]
    pub(crate) fn linear(schema: LogicalSchema) -> Self {
        Self::Linear(schema)
    }

    /// Indexed backend: ordinal table rebuilt from the exact schema state.
    pub(crate) fn indexed(schema: LogicalSchema) -> Self {
        let entries = build_ordinals(&schema);
        Self::Indexed(IndexedSchema { schema, entries })
    }

    pub(crate) fn schema(&self) -> &LogicalSchema {
        match self {
            Self::Linear(schema) => schema,
            Self::Indexed(state) => &state.schema,
        }
    }

    /// Rebuilds the ordinal table from the current schema state (Indexed
    /// variant only). Called at the end of every mutation, in the same call,
    /// so the index can never outlive the schema state it was derived from.
    fn refresh(&mut self) {
        if let Self::Indexed(state) = self {
            state.entries = build_ordinals(&state.schema);
        }
    }

    /// Recomputes the ordinal table and compares it with the held entries.
    /// Runs under `debug_assertions` after every mutation; the test suite
    /// also calls it explicitly under `cfg(test)`.
    pub(crate) fn verify(&self) -> bool {
        match self {
            Self::Linear(_) => true,
            Self::Indexed(state) => build_ordinals(&state.schema) == state.entries,
        }
    }

    pub(crate) fn rename(&mut self, id: ColumnId, to: String) -> Result<(), EngineError> {
        self.schema_mut()
            .rename_column(id, to)
            .map_err(|_| EngineError::UnknownColumn(id))?;
        self.refresh();
        debug_assert!(self.verify());
        Ok(())
    }

    /// Replaces all fields with a newly built, validated field list.
    pub(crate) fn store(
        &mut self,
        fields: Vec<LogicalField>,
        invalid_reason: &'static str,
    ) -> Result<(), EngineError> {
        *self.schema_mut() =
            LogicalSchema::new(fields).map_err(|_| EngineError::InvalidPlan(invalid_reason))?;
        self.refresh();
        debug_assert!(self.verify());
        Ok(())
    }

    /// Adopts an already-validated schema state wholesale.
    pub(crate) fn swap_with(&mut self, schema: LogicalSchema) {
        *self.schema_mut() = schema;
        self.refresh();
        debug_assert!(self.verify());
    }

    pub(crate) fn into_schema(self) -> LogicalSchema {
        match self {
            Self::Linear(schema) => schema,
            Self::Indexed(state) => state.schema,
        }
    }

    fn schema_mut(&mut self) -> &mut LogicalSchema {
        match self {
            Self::Linear(schema) => schema,
            Self::Indexed(state) => &mut state.schema,
        }
    }
}

impl ColumnLookup for WorkingSchema {
    fn lookup_field(&self, id: ColumnId) -> Option<&LogicalField> {
        match self {
            Self::Linear(schema) => schema.field(id),
            Self::Indexed(state) => state
                .entries
                .binary_search_by_key(&id, |(key, _)| *key)
                .ok()
                .and_then(|position| {
                    let ordinal = state.entries[position].1 as usize;
                    state.schema.fields.get(ordinal)
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use stillflow_core::LogicalType;
    use uuid::Uuid;

    #[allow(dead_code)] // kept for literal-tuple cases
    fn schema(fields: &[(u128, &str, LogicalType)]) -> LogicalSchema {
        let fields: Vec<LogicalField> = fields
            .iter()
            .map(|(id, name, ty)| {
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(*id)),
                    *name,
                    ty.clone(),
                    false,
                )
                .expect("field")
            })
            .collect();
        LogicalSchema::new(fields).expect("schema")
    }

    fn wide(f: usize) -> LogicalSchema {
        wide_owned(f)
    }

    // The tuple helper takes owned strings; a thin wrapper keeps the schema
    // builder signature small.
    fn wide_owned(f: usize) -> LogicalSchema {
        let fields: Vec<LogicalField> = (0..f)
            .map(|i| {
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(i as u128 + 1000)),
                    format!("c{i}"),
                    LogicalType::Int64,
                    false,
                )
                .expect("field")
            })
            .collect();
        LogicalSchema::new(fields).expect("schema")
    }

    /// Wide schema with a disjoint id range (2000 + i), names "n{i}".
    fn wide_new_range(f: usize) -> LogicalSchema {
        let fields: Vec<LogicalField> = (0..f)
            .map(|i| {
                LogicalField::new(
                    ColumnId::from_uuid(Uuid::from_u128(i as u128 + 2000)),
                    format!("n{i}"),
                    LogicalType::Int64,
                    false,
                )
                .expect("field")
            })
            .collect();
        LogicalSchema::new(fields).expect("schema")
    }

    #[test]
    fn policy_is_deterministic_and_routes_measured_shapes() {
        // Same shape -> same decision, always.
        assert!(use_index(4096, 48) == use_index(4096, 48));
        assert!(use_index(16, 2048) == use_index(16, 2048));
        // Measured loss region (F=4096 sparse projection, ~16 projected
        // columns -> ~48 served lookups) must stay linear.
        assert!(!use_index(4096, 48));
        assert!(!use_index(4096, 512 - 1));
        // Measured win regions stay indexed: dense/near-full projections and
        // expression-heavy propagations.
        assert!(use_index(4096, 512));
        assert!(use_index(512, 64));
        assert!(use_index(16, 2048));
        // F=64 small control (R=24): measured build overhead exceeds scan
        // savings at the F/8 ratio -> must stay linear.
        assert!(!use_index(64, 24));
        // Only clearly amortized small-schema cases index (floor 32).
        assert!(use_index(64, 32));
        assert!(use_index(8, 32));
        assert!(!use_index(64, 31));
        // Empty/degenerate shapes never index.
        assert!(!use_index(0, 0));
        assert!(!use_index(1, 3));
    }

    #[test]
    fn index_matches_linear_scan_on_every_position() {
        for f in [1usize, 2, 16, 64, 512, 2048] {
            let schema = wide(f);
            let index = OrdinalIndex::build(&schema);
            assert!(index.verify(), "F={f}");
            for i in 0..f {
                let id = ColumnId::from_uuid(Uuid::from_u128(i as u128 + 1000));
                assert_eq!(
                    index.field(id).map(|f| f.name.as_str()),
                    schema.field(id).map(|f| f.name.as_str())
                );
            }
            // Unknown ids resolve identically (None).
            let unknown = ColumnId::from_uuid(Uuid::from_u128(999_999));
            assert_eq!(
                index.field(unknown).is_none(),
                schema.field(unknown).is_none()
            );
        }
    }

    #[test]
    fn duplicate_ids_resolve_first_wins_in_both_backends() {
        // `LogicalSchema::new` validates uniqueness, so a duplicate-id schema
        // can only exist unvalidated. The index must still match the linear
        // first-match resolution on such input (defense in depth; validated
        // schemas have unique ids and never hit this branch).
        let field = |id: u128, name: &str| {
            LogicalField::new(
                ColumnId::from_uuid(Uuid::from_u128(id)),
                name.to_owned(),
                LogicalType::Int64,
                false,
            )
            .expect("field")
        };
        let schema = LogicalSchema {
            version: 1,
            fields: vec![field(1, "first"), field(2, "second"), field(1, "dupe")],
            metadata: BTreeMap::new(),
        };
        let index = OrdinalIndex::build(&schema);
        let id = ColumnId::from_uuid(Uuid::from_u128(1));
        assert_eq!(index.field(id).unwrap().name, "first");
        assert_eq!(schema.field(id).unwrap().name, "first");
    }

    #[test]
    fn working_schema_linear_and_indexed_agree_and_never_go_stale() {
        let start = wide(8);
        let mut linear = WorkingSchema::linear(start.clone());
        let mut indexed = WorkingSchema::indexed(start.clone());

        // Rule-family mutations in sequence; after each, both backends agree
        // and the indexed backend is consistent with its schema.
        macro_rules! step {
            ($action:expr) => {{
                let a = $action(&mut linear);
                let b = $action(&mut indexed);
                assert_eq!(a.is_ok(), b.is_ok(), "outcome parity");
                if a.is_ok() && b.is_ok() {
                    assert_eq!(
                        linear.schema().canonical_bytes().expect("canonical"),
                        indexed.schema().canonical_bytes().expect("canonical"),
                        "canonical schema identity"
                    );
                    assert!(indexed.verify(), "index consistent after mutation");
                    // Every lookup resolves the NEW mapping in both backends.
                    for id in [1000u128, 1001, 1003, 1005, 1007] {
                        let id = ColumnId::from_uuid(Uuid::from_u128(id));
                        assert_eq!(
                            linear.lookup_field(id).map(|f| f.name.clone()),
                            indexed.lookup_field(id).map(|f| f.name.clone()),
                            "lookup parity after mutation"
                        );
                    }
                } else {
                    // Errors must agree in variant and message.
                    assert_eq!(format!("{a:?}"), format!("{b:?}"));
                }
            }};
        }

        let id = |i: u128| ColumnId::from_uuid(Uuid::from_u128(i));
        step!(|w: &mut WorkingSchema| w.rename(id(1000), "renamed".to_owned()));
        step!(|w: &mut WorkingSchema| w.store(
            w.schema().fields.clone(),
            "store produced an invalid schema"
        ));
        step!(|w: &mut WorkingSchema| w.store(
            w.schema()
                .fields
                .iter()
                .filter(|field| field.id != id(1002))
                .cloned()
                .collect(),
            "drop produced an invalid schema"
        ));
        step!(|w: &mut WorkingSchema| w.rename(id(999_999), "nope".to_owned()));
        step!(|w: &mut WorkingSchema| w.store(Vec::new(), "store produced an invalid schema"));
        step!(|w: &mut WorkingSchema| -> Result<(), EngineError> {
            // Swap in a schema with a DISJOINT id range: the index must
            // reflect the swapped-in schema, never the old one.
            w.swap_with(wide_new_range(4));
            Ok(())
        });
        // After swap, the index must resolve the new mapping and forget the
        // old schema's ids entirely.
        assert_eq!(
            indexed.lookup_field(id(2003)).map(|f| f.name.as_str()),
            Some("n3")
        );
        assert_eq!(indexed.lookup_field(id(1000)), None);
    }

    #[test]
    fn for_shape_picks_the_policy_consistently() {
        let big = wide(4096);
        let ws = WorkingSchema::for_shape(big.clone(), 48);
        assert!(matches!(ws, WorkingSchema::Linear(_)));
        let ws = WorkingSchema::for_shape(big.clone(), 4096 * 3);
        assert!(matches!(ws, WorkingSchema::Indexed(_)));
        let auth = AuthorizedLookup::for_shape(&big, 48);
        assert!(matches!(auth, AuthorizedLookup::Linear(_)));
    }
}
