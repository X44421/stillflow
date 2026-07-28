//! Domain model and shared data contracts for the Stillflow ingestion
//! backend.
//!
//! `stillflow-core` owns the source-of-truth types that every other crate
//! builds on: sessions, objects, datasets, snapshots, schema descriptors
//! and typed errors. It depends on no other workspace crate, and Apache
//! Arrow is the interchange protocol at its boundary.
//!
//! The domain model itself lands with the connector interface (see
//! `docs/data-ingestion-architecture.md`); this crate currently only
//! establishes the workspace wiring.

use arrow::datatypes::Schema;

/// Returns an empty Arrow schema.
///
/// Placeholder proving that the Arrow interchange dependency is wired
/// through the workspace; real schema descriptors arrive with the domain
/// model.
pub fn empty_schema() -> Schema {
    Schema::empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_schema_has_no_fields() {
        assert!(empty_schema().fields().is_empty());
    }
}
