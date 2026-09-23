//! Polars compatibility adapter for the local tabular connector (P55-A1).
//!
//! Every place where this connector touches a Polars API whose shape changed
//! between 0.46 and 0.55 lives in this module, so the connector keeps talking
//! to StillFlow-owned interfaces instead of Polars internals.

pub(crate) mod csv_batch;
