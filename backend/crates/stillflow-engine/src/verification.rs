//! Engine-owned E4-S2 verification materialization surface (contract
//! sections 5, 6, 10, 11, and 12 of
//! `docs/issues/issue-054-validation-rejected-rows-contract.md`).
//!
//! This module implements exactly the Proposed API frozen in contract
//! section 11 that E4-S1 did not already deliver: the Engine-owned
//! constants, [`VerificationIdentities`], [`VerificationRequest`], the
//! canonical dedup-key encoder of section 6.4, and
//! [`ExecutionEngine::materialize_verification`].
//!
//! Storage publication, bundle atomicity, recovery, and the typed dedup
//! index were delivered and accepted in E4-S1 (PR #74); this path only
//! consumes those surfaces.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use stillflow_core::verification::{
    ArtifactKind, ArtifactProvenanceDraft, ArtifactProvenanceInput, LogicalInputRef,
    VERIFICATION_CONTRACT_VERSION,
};

use stillflow_core::{LogicalType, RequestContext, SourceAsset, SourceConnection, TimeUnit};
use stillflow_plan::LogicalPlan;
use stillflow_storage::{SnapshotStore, MAX_DEDUP_KEY_BYTES};
use uuid::Uuid;

use crate::engine::ExecutionEngine;
use crate::error::map_context_error;
use crate::{EngineError, MAX_RULES_PER_NODE};

/// Ordered key columns per `Deduplicate` rule (contract 11/14).
pub const MAX_DEDUP_KEY_COLUMNS: usize = 64;

/// Live columnar payload ceiling of the verification path (contract 12.1).
pub const VERIFICATION_MAX_LIVE_COLUMNAR_PAYLOADS: u8 = 6;
/// Engine peak of the verification path: 4 * 64 MiB + 2 * 2 MiB + 5 MiB
/// (contract 12.1 / 14).
pub const VERIFICATION_MAX_ENGINE_PEAK_BYTES: usize = (4 * stillflow_core::MAX_BATCH_BYTES)
    + (2 * stillflow_storage::artifact::REPORT_PACK_BYTES)
    + crate::MAX_OPERATOR_STATE_BYTES;
/// Compiled-plan budget inside the verification operator state (contract 12.2).
pub const VERIFICATION_MAX_COMPILED_PLAN_BYTES: usize = 3 * 1024 * 1024;
/// Routing metadata budget: ordinals, masks, counters, finding buffers
/// (contract 12.2).
pub const VERIFICATION_MAX_ROUTING_STATE_BYTES: usize = 512 * 1024;
/// Findings emitted for one source row across the whole run (contract 5.2).
pub const MAX_VALIDATION_FINDINGS_PER_ROW: usize = MAX_RULES_PER_NODE;
/// Validation message budget after trim (contract 10.7).
pub const MAX_VALIDATION_MESSAGE_BYTES: usize = 1_024;

/// Caller-injected identities for one verification run (contract 10.5/11).
///
/// Every id and timestamp is supplied by the caller; the engine never
/// generates identities or wall-clock timestamps.
#[derive(Debug, Clone)]
pub struct VerificationIdentities {
    pub run_id: Uuid,
    pub bundle_id: Uuid,
    pub bundle_artifact_id: Uuid,
    pub snapshot_id: Uuid,
    pub dataset_id: Uuid,
    pub validation_report_artifact_id: Uuid,
    /// Deterministic protocol (contract 10.5): `Some(id)` authorizes the
    /// rejected artifact under that id when terminal rejections occur and
    /// stays unused without error at zero rejections. `None` declares the
    /// run must reject nothing; the first terminal rejection fails with
    /// `EngineError::InvalidPlan` before any rejected writer append.
    pub rejected_rows_artifact_id: Option<Uuid>,
    pub deduplication_report_artifact_id: Uuid,
    pub session_id: Uuid,
    pub logical_input: LogicalInputRef,
    /// Caller-supplied expected canonical-plan SHA-256; the engine
    /// recomputes the digest over `LogicalPlan::canonical_bytes()`,
    /// rejects a mismatch, and inserts the recomputed value into every
    /// provenance draft (contract 10.5).
    pub canonical_plan_digest: [u8; 32],
    pub created_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub committed_at: DateTime<Utc>,
    pub lineage: BTreeSet<Uuid>,
    pub quality_score: Option<u8>,
}

/// One deterministic verification materialization request (contract 11).
#[derive(Debug)]
pub struct VerificationRequest<'a> {
    pub plan: LogicalPlan,
    pub connection: SourceConnection,
    pub asset: SourceAsset,
    pub schema_override: Option<stillflow_core::LogicalSchema>,
    pub identities: VerificationIdentities,
    pub context: RequestContext,
    pub batch_size: usize,
    pub store: &'a SnapshotStore,
}

/// Validates caller-injected identities before any storage I/O
/// (contract 10.5). Pairwise distinctness is checked again by
/// `VerificationBundleDraft::try_new`; this pre-check keeps the failure
/// inside the Engine error vocabulary.
pub(crate) fn validate_verification_identities(
    identities: &VerificationIdentities,
    source_asset_id: Uuid,
) -> Result<(), EngineError> {
    let mut distinct = [
        Some(identities.run_id),
        Some(identities.bundle_id),
        Some(identities.bundle_artifact_id),
        Some(identities.snapshot_id),
        Some(identities.dataset_id),
        Some(identities.validation_report_artifact_id),
        identities.rejected_rows_artifact_id,
        Some(identities.deduplication_report_artifact_id),
        Some(identities.session_id),
    ];
    distinct.sort_unstable();
    if identities.run_id.is_nil()
        || identities.bundle_id.is_nil()
        || identities.bundle_artifact_id.is_nil()
        || identities.snapshot_id.is_nil()
        || identities.dataset_id.is_nil()
        || identities.validation_report_artifact_id.is_nil()
        || identities
            .rejected_rows_artifact_id
            .is_some_and(|id: Uuid| Uuid::is_nil(&id))
        || identities.deduplication_report_artifact_id.is_nil()
        || identities.session_id.is_nil()
        || source_asset_id.is_nil()
        || identities.lineage.iter().any(Uuid::is_nil)
    {
        return Err(EngineError::InvalidPlan(
            "injected verification identities must not be nil",
        ));
    }
    if distinct.windows(2).any(|window| window[0] == window[1]) {
        return Err(EngineError::InvalidPlan(
            "run, bundle, and artifact identities must be pairwise distinct",
        ));
    }
    if identities.quality_score.is_some_and(|score| score > 100) {
        return Err(EngineError::InvalidPlan("quality score is outside 0..=100"));
    }
    if identities.created_at > identities.started_at
        || identities.started_at > identities.committed_at
    {
        return Err(EngineError::InvalidPlan(
            "injected timestamps must be non-decreasing created_at <= started_at <= committed_at",
        ));
    }
    Ok(())
}

/// Assembles the bundle provenance draft with the verified canonical-plan
/// digest (contract 7.2 / 10.5). Callers cannot forge engine-derived
/// integrity fields: the digest here is the recomputed one.
pub(crate) fn bundle_provenance_draft(
    identities: &VerificationIdentities,
    asset_id: Uuid,
    plan_fingerprint: [u8; 32],
    canonical_plan_digest: [u8; 32],
    engine_build: &'static str,
) -> ArtifactProvenanceDraft {
    ArtifactProvenanceDraft {
        input: ArtifactProvenanceInput {
            run_id: identities.run_id,
            bundle_id: identities.bundle_id,
            artifact_id: identities.bundle_artifact_id,
            artifact_kind: ArtifactKind::VerificationBundle,
            session_id: identities.session_id,
            input: LogicalInputRef {
                input: stillflow_core::verification::InputRef::Asset { asset_id },
                version_digest: identities.logical_input.version_digest,
            },
            lineage: identities.lineage.clone(),
            created_at: identities.created_at,
            started_at: identities.started_at,
            committed_at: identities.committed_at,
        },
        plan_fingerprint,
        canonical_plan_digest,
        engine_contract_version: crate::ENGINE_CONTRACT_VERSION,
        engine_build: engine_build.to_string(),
        verification_contract_version: VERIFICATION_CONTRACT_VERSION,
    }
}

/// One typed dedup-key component value extracted from the working set
/// (contract 6.2/6.4). The declared working type governs the emitted tag;
/// the variant must match it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyValue<'a> {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Utf8(&'a str),
    Utf8Owned(String),
    Binary(&'a [u8]),
    Date32(i32),
    Timestamp {
        epoch: i64,
        unit: TimeUnit,
        timezone: Option<&'a str>,
    },
}

/// Running-length aborting byte sink for canonical key encoding
/// (contract 6.1/6.4): the encoder aborts before extending an allocation
/// past [`MAX_DEDUP_KEY_BYTES`] and the complete encoded length is
/// re-checked immediately before the SQLite insert.
pub(crate) struct KeyBytes {
    bytes: Vec<u8>,
}

impl KeyBytes {
    pub(crate) fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn push(&mut self, byte: u8) -> Result<(), EngineError> {
        if self.bytes.len() >= MAX_DEDUP_KEY_BYTES {
            return Err(EngineError::BoundExceeded(
                "encoded dedup key exceeds MAX_DEDUP_KEY_BYTES",
            ));
        }
        self.bytes.push(byte);
        Ok(())
    }

    fn extend_from_slice(&mut self, slice: &[u8]) -> Result<(), EngineError> {
        let remaining = MAX_DEDUP_KEY_BYTES - self.bytes.len();
        if slice.len() > remaining {
            return Err(EngineError::BoundExceeded(
                "encoded dedup key exceeds MAX_DEDUP_KEY_BYTES",
            ));
        }
        self.bytes.extend_from_slice(slice);
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

/// Canonicalizes one IEEE float to its frozen encoding bits (contract
/// 6.4): every NaN becomes the single canonical quiet NaN, every zero
/// becomes positive zero, finite values keep exact bits.
pub(crate) fn canonical_float32_bits(value: f32) -> u32 {
    if value.is_nan() {
        0x7FC0_0000
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

pub(crate) fn canonical_float64_bits(value: f64) -> u64 {
    if value.is_nan() {
        0x7FF8_0000_0000_0000
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// Encodes one key component with the frozen injective encoding of
/// contract section 6.4 into `out`. The declared working type selects the
/// tag; paused E4-C0 types are rejected defensively (preflight already
/// refuses them).
pub(crate) fn encode_component<'a>(
    declared: &'a LogicalType,
    value: KeyValue<'a>,
    out: &mut KeyBytes,
) -> Result<(), EngineError> {
    match declared {
        LogicalType::List(_) | LogicalType::Struct(_) => {
            return Err(EngineError::TypeError(
                "list and struct keys are paused in E4-C0",
            ));
        }
        LogicalType::Timestamp {
            unit: TimeUnit::Second,
            ..
        } => {
            return Err(EngineError::TypeError(
                "Timestamp Second keys are paused in E4-C0",
            ));
        }
        _ => {}
    }
    if matches!(value, KeyValue::Null) {
        return out.push(0x00);
    }
    let tag: u8 = match declared {
        LogicalType::Null => {
            return out.push(0x00);
        }
        LogicalType::Boolean => 0x01,
        LogicalType::Int8 => 0x02,
        LogicalType::Int16 => 0x03,
        LogicalType::Int32 => 0x04,
        LogicalType::Int64 => 0x05,
        LogicalType::UInt8 => 0x06,
        LogicalType::UInt16 => 0x07,
        LogicalType::UInt32 => 0x08,
        LogicalType::UInt64 => 0x09,
        LogicalType::Float32 => 0x0A,
        LogicalType::Float64 => 0x0B,
        LogicalType::Utf8 => 0x0C,
        LogicalType::Binary => 0x0D,
        LogicalType::Date32 => 0x0E,
        LogicalType::Timestamp { unit, .. } => match unit {
            TimeUnit::Millisecond => 0x0F,
            TimeUnit::Microsecond => 0x0F,
            TimeUnit::Nanosecond => 0x0F,
            TimeUnit::Second => {
                return Err(EngineError::TypeError(
                    "Timestamp Second keys are paused in E4-C0",
                ))
            }
        },
        LogicalType::List(_) | LogicalType::Struct(_) => {
            return Err(EngineError::TypeError(
                "list and struct keys are paused in E4-C0",
            ));
        }
    };
    out.push(tag)?;
    match (declared, value) {
        (LogicalType::Boolean, KeyValue::Boolean(inner)) => out.push(u8::from(inner)),
        (LogicalType::Int8, KeyValue::Int8(inner)) => out.push(inner as u8),
        (LogicalType::Int16, KeyValue::Int16(inner)) => out.extend_from_slice(&inner.to_le_bytes()),
        (LogicalType::Int32, KeyValue::Int32(inner)) => out.extend_from_slice(&inner.to_le_bytes()),
        (LogicalType::Int64, KeyValue::Int64(inner)) => out.extend_from_slice(&inner.to_le_bytes()),
        (LogicalType::UInt8, KeyValue::UInt8(inner)) => out.push(inner),
        (LogicalType::UInt16, KeyValue::UInt16(inner)) => {
            out.extend_from_slice(&inner.to_le_bytes())
        }
        (LogicalType::UInt32, KeyValue::UInt32(inner)) => {
            out.extend_from_slice(&inner.to_le_bytes())
        }
        (LogicalType::UInt64, KeyValue::UInt64(inner)) => {
            out.extend_from_slice(&inner.to_le_bytes())
        }
        (LogicalType::Float32, KeyValue::Float32(inner)) => {
            out.extend_from_slice(&canonical_float32_bits(inner).to_le_bytes())
        }
        (LogicalType::Float64, KeyValue::Float64(inner)) => {
            out.extend_from_slice(&canonical_float64_bits(inner).to_le_bytes())
        }
        (LogicalType::Utf8, KeyValue::Utf8(inner)) => {
            out.extend_from_slice(&(inner.len() as u32).to_le_bytes())?;
            out.extend_from_slice(inner.as_bytes())
        }
        (LogicalType::Utf8, KeyValue::Utf8Owned(ref inner)) => {
            out.extend_from_slice(&(inner.len() as u32).to_le_bytes())?;
            out.extend_from_slice(inner.as_bytes())
        }
        (LogicalType::Binary, KeyValue::Binary(inner)) => {
            out.extend_from_slice(&(inner.len() as u32).to_le_bytes())?;
            out.extend_from_slice(inner)
        }
        (LogicalType::Date32, KeyValue::Date32(inner)) => {
            out.extend_from_slice(&inner.to_le_bytes())
        }
        (
            LogicalType::Timestamp { unit, timezone },
            KeyValue::Timestamp {
                epoch,
                unit: value_unit,
                timezone: value_timezone,
            },
        ) => {
            if *unit != value_unit {
                return Err(EngineError::Internal(
                    "dedup key timestamp unit drifted from the working schema",
                ));
            }
            let unit_tag: u8 = match unit {
                TimeUnit::Millisecond => 0x01,
                TimeUnit::Microsecond => 0x02,
                TimeUnit::Nanosecond => 0x03,
                TimeUnit::Second => {
                    return Err(EngineError::TypeError(
                        "Timestamp Second keys are paused in E4-C0",
                    ))
                }
            };
            out.push(unit_tag)?;
            match (timezone, value_timezone) {
                (None, None) => out.push(0x00)?,
                (Some(expected), Some(actual)) => {
                    if expected != actual {
                        return Err(EngineError::Internal(
                            "dedup key timestamp timezone drifted from the working schema",
                        ));
                    }
                    out.push(0x01)?;
                    out.extend_from_slice(&(expected.len() as u32).to_le_bytes())?;
                    out.extend_from_slice(expected.as_bytes())?;
                }
                _ => {
                    return Err(EngineError::Internal(
                        "dedup key timestamp timezone presence drifted from the working schema",
                    ));
                }
            }
            out.extend_from_slice(&epoch.to_le_bytes())
        }
        _ => Err(EngineError::Internal(
            "dedup key component value does not match its declared working type",
        )),
    }
}

#[cfg(test)]
mod key_encoding_tests {
    use super::*;

    fn encoded(declared: &LogicalType, value: KeyValue<'_>) -> Vec<u8> {
        let mut out = KeyBytes::new();
        encode_component(declared, value, &mut out).expect("encode component");
        out.as_slice().to_vec()
    }

    #[test]
    fn null_is_the_single_zero_byte_for_every_declared_type() {
        for declared in [
            LogicalType::Null,
            LogicalType::Boolean,
            LogicalType::Int64,
            LogicalType::Float64,
            LogicalType::Utf8,
            LogicalType::Binary,
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".to_string()),
            },
        ] {
            assert_eq!(encoded(&declared, KeyValue::Null), vec![0x00]);
        }
    }

    #[test]
    fn golden_vectors_cover_every_supported_tag() {
        assert_eq!(
            encoded(&LogicalType::Boolean, KeyValue::Boolean(true)),
            vec![0x01, 0x01]
        );
        assert_eq!(
            encoded(&LogicalType::Boolean, KeyValue::Boolean(false)),
            vec![0x01, 0x00]
        );
        assert_eq!(
            encoded(&LogicalType::Int8, KeyValue::Int8(-1)),
            vec![0x02, 0xFF]
        );
        assert_eq!(
            encoded(&LogicalType::Int16, KeyValue::Int16(-2)),
            vec![0x03, 0xFE, 0xFF]
        );
        assert_eq!(
            encoded(&LogicalType::Int32, KeyValue::Int32(1)),
            vec![0x04, 0x01, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            encoded(&LogicalType::Int64, KeyValue::Int64(-1)),
            vec![0x05, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(
            encoded(&LogicalType::UInt8, KeyValue::UInt8(7)),
            vec![0x06, 0x07]
        );
        assert_eq!(
            encoded(&LogicalType::UInt16, KeyValue::UInt16(258)),
            vec![0x07, 0x02, 0x01]
        );
        assert_eq!(
            encoded(&LogicalType::UInt32, KeyValue::UInt32(1)),
            vec![0x08, 0x01, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            encoded(&LogicalType::UInt64, KeyValue::UInt64(1)),
            vec![0x09, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            encoded(&LogicalType::Float32, KeyValue::Float32(1.0)),
            vec![0x0A, 0x00, 0x00, 0x80, 0x3F]
        );
        assert_eq!(
            encoded(&LogicalType::Float64, KeyValue::Float64(1.0)),
            vec![0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F]
        );
        assert_eq!(
            encoded(&LogicalType::Utf8, KeyValue::Utf8("é")),
            vec![0x0C, 0x02, 0x00, 0x00, 0x00, 0xC3, 0xA9]
        );
        assert_eq!(
            encoded(&LogicalType::Binary, KeyValue::Binary(&[0xDE, 0xAD])),
            vec![0x0D, 0x02, 0x00, 0x00, 0x00, 0xDE, 0xAD]
        );
        assert_eq!(
            encoded(&LogicalType::Date32, KeyValue::Date32(-1)),
            vec![0x0E, 0xFF, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(
            encoded(
                &LogicalType::Timestamp {
                    unit: TimeUnit::Millisecond,
                    timezone: None,
                },
                KeyValue::Timestamp {
                    epoch: 1_000,
                    unit: TimeUnit::Millisecond,
                    timezone: None,
                },
            ),
            vec![0x0F, 0x01, 0x00, 0xE8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            encoded(
                &LogicalType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some("UTC".to_string()),
                },
                KeyValue::Timestamp {
                    epoch: 5,
                    unit: TimeUnit::Microsecond,
                    timezone: Some("UTC"),
                },
            ),
            vec![
                0x0F, 0x02, 0x01, 0x03, 0x00, 0x00, 0x00, b'U', b'T', b'C', 0x05, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
            ]
        );
    }

    #[test]
    fn timestamp_presence_byte_separates_none_from_any_some_encoding() {
        let none = encoded(
            &LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None,
            },
            KeyValue::Timestamp {
                epoch: 9,
                unit: TimeUnit::Nanosecond,
                timezone: None,
            },
        );
        let some = encoded(
            &LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: Some("X".to_string()),
            },
            KeyValue::Timestamp {
                epoch: 9,
                unit: TimeUnit::Nanosecond,
                timezone: Some("X"),
            },
        );
        assert_ne!(none, some);
        assert_eq!(none[2], 0x00);
        assert_eq!(some[2], 0x01);
    }

    #[test]
    fn nan_payloads_group_and_zeros_are_positive() {
        let quiet = encoded(&LogicalType::Float64, KeyValue::Float64(f64::NAN));
        let signaling = f64::from_bits(0xFFF8_0000_0000_0001);
        assert_eq!(
            quiet,
            encoded(&LogicalType::Float64, KeyValue::Float64(signaling))
        );
        assert_eq!(
            encoded(&LogicalType::Float64, KeyValue::Float64(-0.0)),
            encoded(&LogicalType::Float64, KeyValue::Float64(0.0))
        );
        assert_eq!(
            encoded(&LogicalType::Float32, KeyValue::Float32(-0.0)),
            encoded(&LogicalType::Float32, KeyValue::Float32(0.0))
        );
        let nan_bits = &quiet[1..];
        assert_eq!(nan_bits, &0x7FF8_0000_0000_0000u64.to_le_bytes());
    }

    #[test]
    fn finite_values_keep_exact_bits_and_stay_distinct_from_zero() {
        assert_ne!(
            encoded(&LogicalType::Float64, KeyValue::Float64(0.1)),
            encoded(&LogicalType::Float64, KeyValue::Float64(0.0))
        );
        assert_eq!(
            encoded(&LogicalType::Float32, KeyValue::Float32(1.5)),
            vec![0x0A, 0x00, 0x00, 0xC0, 0x3F]
        );
    }

    #[test]
    fn empty_utf8_and_binary_are_distinct_from_null_and_from_each_other() {
        let null = encoded(&LogicalType::Utf8, KeyValue::Null);
        let empty = encoded(&LogicalType::Utf8, KeyValue::Utf8(""));
        assert_ne!(null, empty);
        assert_eq!(empty, vec![0x0C, 0x00, 0x00, 0x00, 0x00]);
        let binary_empty = encoded(&LogicalType::Binary, KeyValue::Binary(&[]));
        assert_eq!(binary_empty, vec![0x0D, 0x00, 0x00, 0x00, 0x00]);
        assert_ne!(empty, binary_empty);
    }

    #[test]
    fn same_value_different_component_type_is_distinct() {
        assert_ne!(
            encoded(&LogicalType::Int32, KeyValue::Int32(1)),
            encoded(&LogicalType::Int64, KeyValue::Int64(1))
        );
    }

    #[test]
    fn paused_types_are_refused_and_never_emit_reserved_tags() {
        let mut out = KeyBytes::new();
        assert!(encode_component(
            &LogicalType::List(Box::new(LogicalType::Int64)),
            KeyValue::Null,
            &mut out
        )
        .is_err());
        assert!(encode_component(&LogicalType::Struct(vec![]), KeyValue::Null, &mut out).is_err());
        assert!(encode_component(
            &LogicalType::Timestamp {
                unit: TimeUnit::Second,
                timezone: None,
            },
            KeyValue::Timestamp {
                epoch: 0,
                unit: TimeUnit::Second,
                timezone: None,
            },
            &mut out
        )
        .is_err());
    }

    #[test]
    fn running_length_counter_aborts_before_extending_past_the_cap() {
        let mut out = KeyBytes::new();
        let long = "x".repeat(MAX_DEDUP_KEY_BYTES);
        let error = encode_component(&LogicalType::Utf8, KeyValue::Utf8(&long), &mut out)
            .expect_err("must abort at the cap");
        assert!(matches!(error, EngineError::BoundExceeded(_)));
        assert!(out.len() <= MAX_DEDUP_KEY_BYTES);
    }

    #[test]
    fn exact_cap_boundary_is_accepted_and_one_more_byte_aborts() {
        let mut out = KeyBytes::new();
        let exact = "x".repeat(MAX_DEDUP_KEY_BYTES - 5);
        encode_component(&LogicalType::Utf8, KeyValue::Utf8(&exact), &mut out)
            .expect("fits exactly");
        assert_eq!(out.len(), MAX_DEDUP_KEY_BYTES);
        let mut next = KeyBytes::new();
        let overflow = "y".repeat(MAX_DEDUP_KEY_BYTES - 4);
        assert!(
            encode_component(&LogicalType::Utf8, KeyValue::Utf8(&overflow), &mut next).is_err()
        );
    }
}

// ---------------------------------------------------------------------------
// E4 verification preflight (contract sections 5, 6, 8.6/8.7, 10.1, 10.7)
// ---------------------------------------------------------------------------

use futures::StreamExt;
use stillflow_core::{ColumnId, Expr};
use stillflow_plan::{PlanNodeKind, Rule, ValidationSeverity};

/// One engine-executed verification step. Routing rules become their own
/// steps so the streaming loop can interleave row-level decisions with
/// batch transforms; contiguous non-routing rules stay grouped for the
/// shared Polars lowering.
#[derive(Debug)]
pub(crate) enum VStep {
    Project {
        columns: Vec<ColumnId>,
    },
    Filter {
        predicate: Expr,
    },
    FilterRows {
        predicate: Expr,
    },
    TransformRules {
        rules: Vec<Rule>,
    },
    Validate {
        node_id: Uuid,
        rule_ordinal: u32,
        predicate: Expr,
        severity: ValidationSeverity,
        message: String,
    },
    Deduplicate {
        node_id: Uuid,
        rule_ordinal: u32,
        keys: Vec<ColumnId>,
        key_types: Vec<LogicalType>,
    },
}

/// The verification-compiled plan: ordered engine steps plus the index of
/// the first step that consumes logical Scan output rows (contract 5.1:
/// ordinals are assigned after Scan projection and `Scan.predicate`).
#[derive(Debug)]
pub(crate) struct VerificationPlan {
    pub(crate) vsteps: StdArc<Vec<VStep>>,
    pub(crate) scan_boundary: usize,
}

/// Reserved report/rejected-row control field names (contract 8.6/8.7).
const RESERVED_CONTROL_NAMES: &[&str] = &[
    "input_kind",
    "input_id",
    "input_version_digest",
    "plan_fingerprint",
    "canonical_plan_digest",
    "node_id",
    "rule_ordinal",
    "message",
    "evaluated_count",
    "pass_count",
    "fail_count",
    "warning_count",
    "error_count",
    "null_count",
    "false_count",
    "source_row_ordinal",
    "severity",
    "predicate_outcome",
    "rejection_kind",
    "key_column_count",
    "unique_count",
    "duplicate_count",
    "first_source_row_ordinal",
    "encoded_key_byte_count",
];

/// Every reserved control ColumnId of the frozen `0xE4C0…` namespace
/// (contract 8.6/8.7 shorthand `0x…00NN`). Mirrors the constants in
/// `stillflow_core::verification`.
fn reserved_control_ids() -> std::collections::BTreeSet<Uuid> {
    let mut ids = std::collections::BTreeSet::new();
    let ranges = [
        0x11..=0x19_u32,
        0x21..=0x2F,
        0x31..=0x3A,
        0x41..=0x4B,
        0x51..=0x5B,
    ];
    for range in ranges {
        for suffix in range {
            ids.insert(Uuid::from_u128(
                0xE4C0_0000_0000_4000_8000_0000_0000_0000 + u128::from(suffix),
            ));
        }
    }
    ids
}

fn reject_reserved_collision(schema: &stillflow_core::LogicalSchema) -> Result<(), EngineError> {
    let reserved_ids = reserved_control_ids();
    for field in &schema.fields {
        if reserved_ids.contains(&field.id.as_uuid())
            || RESERVED_CONTROL_NAMES
                .iter()
                .any(|name| *name == field.name)
        {
            return Err(EngineError::InvalidPlan(
                "source schema collides with reserved verification control identities",
            ));
        }
    }
    Ok(())
}

/// Static maximum encoded length of one fixed-width key component
/// (contract 6.1); `None` marks a variable-width component.
fn static_component_max(declared: &LogicalType) -> Option<usize> {
    let payload: usize = match declared {
        LogicalType::Null => 0,
        LogicalType::Boolean | LogicalType::Int8 | LogicalType::UInt8 => 1,
        LogicalType::Int16 | LogicalType::UInt16 => 2,
        LogicalType::Int32 | LogicalType::UInt32 | LogicalType::Float32 | LogicalType::Date32 => 4,
        LogicalType::Int64 | LogicalType::UInt64 | LogicalType::Float64 => 8,
        LogicalType::Utf8 | LogicalType::Binary => return None,
        LogicalType::Timestamp { timezone, .. } => {
            let unit_tag = 1usize;
            let presence = 1usize;
            let timezone_len = match timezone {
                None => 0,
                Some(value) => 4 + value.len(),
            };
            return Some(1 + unit_tag + presence + timezone_len + 8);
        }
        LogicalType::List(_) | LogicalType::Struct(_) => {
            // Paused in E4-C0; rejected before this helper runs.
            return None;
        }
    };
    Some(1 + payload)
}

fn validate_message_contract(message: &str) -> Result<String, EngineError> {
    let trimmed = message.trim();
    if trimmed.is_empty() {
        return Err(EngineError::InvalidPlan(
            "validation message must not be empty",
        ));
    }
    if trimmed.len() > MAX_VALIDATION_MESSAGE_BYTES {
        return Err(EngineError::InvalidPlan(
            "validation message exceeds MAX_VALIDATION_MESSAGE_BYTES",
        ));
    }
    stillflow_core::ensure_no_secret_fields(&serde_json::Value::String(trimmed.to_string()))
        .map_err(|_| EngineError::InvalidPlan("validation message carries secret-like fields"))?;
    stillflow_core::Expr::Literal(stillflow_core::ScalarValue::Utf8(trimmed.to_string()))
        .validate_shape()
        .map_err(|_| EngineError::InvalidPlan("validation message is not a valid literal"))?;
    Ok(trimmed.to_string())
}

fn reject_paused_key_type(declared: &LogicalType) -> Result<(), EngineError> {
    match declared {
        LogicalType::List(_) | LogicalType::Struct(_) => Err(EngineError::TypeError(
            "list and struct keys are paused in E4-C0",
        )),
        LogicalType::Timestamp {
            unit: TimeUnit::Second,
            ..
        } => Err(EngineError::TypeError(
            "Timestamp Second keys are paused in E4-C0",
        )),
        _ => Ok(()),
    }
}

/// Builds the verification-compiled plan and applies every E4-specific
/// preflight check on top of the already-run shared preflight (contract
/// 10.1 step 2 and section 6.1): predicate Boolean inference against the
/// evolving working schema, message safety, key existence/type/count/
/// static-bound checks, and reserved control identity collisions.
pub(crate) fn build_verification_plan(
    plan: &LogicalPlan,
    prepared: &super::preflight::PreparedPlan,
) -> Result<VerificationPlan, EngineError> {
    let linear = super::preflight::linearize(plan)?;
    let mut vsteps: Vec<VStep> = Vec::new();

    // Leading scan-output steps: in-engine projection (when the connector
    // could not project) followed by the in-engine Scan predicate.
    if !prepared.push_projection {
        vsteps.push(VStep::Project {
            columns: prepared.scan_projection.clone(),
        });
    }
    let scan_predicate = match &linear[0].1.kind {
        PlanNodeKind::Scan { predicate, .. } => predicate.clone(),
        _ => return Err(EngineError::Internal("verification scan node missing")),
    };
    if let Some(predicate) = scan_predicate {
        vsteps.push(VStep::Filter { predicate });
    }
    let scan_boundary = vsteps.len();

    let mut working = prepared.expected_connector.clone();
    for (index, step) in vsteps.iter().enumerate() {
        match step {
            VStep::Project { columns } => {
                working = crate::preflight::project_schema(&working, columns)?;
            }
            VStep::Filter { predicate } => {
                crate::typing::require_boolean_in(predicate, &working)?;
            }
            _ => {
                let _ = index;
            }
        }
    }

    // The rejected artifact embeds the logical Scan output schema; its
    // identities must not collide with the frozen controls (contract
    // 8.6/8.7), and at most MAX_SCHEMA_FIELDS - 9 source fields exist.
    reject_reserved_collision(&prepared.scan_output)?;
    if prepared.scan_output.fields.len() + 9 > stillflow_core::MAX_SCHEMA_FIELDS {
        return Err(EngineError::InvalidPlan(
            "source schema leaves no room for the nine rejected-row control fields",
        ));
    }

    for (node_id, node) in linear.iter().skip(1).take(linear.len().saturating_sub(2)) {
        match &node.kind {
            PlanNodeKind::Project { columns } => {
                vsteps.push(VStep::Project {
                    columns: columns.clone(),
                });
                working = crate::preflight::project_schema(&working, columns)?;
            }
            PlanNodeKind::Filter { predicate } => {
                crate::typing::require_boolean_in(predicate, &working)?;
                vsteps.push(VStep::Filter {
                    predicate: predicate.clone(),
                });
            }
            PlanNodeKind::ApplyRules { rules } => {
                let mut transform_group: Vec<Rule> = Vec::new();
                for (rule_ordinal, rule) in rules.iter().enumerate() {
                    let rule_ordinal = u32::try_from(rule_ordinal)
                        .map_err(|_| EngineError::Internal("rule ordinal overflow"))?;
                    match rule {
                        Rule::Validate {
                            predicate,
                            severity,
                            message,
                        } => {
                            flush_transform(&mut vsteps, &mut transform_group);
                            crate::typing::require_boolean_in(predicate, &working)?;
                            let stored_message = validate_message_contract(message)?;
                            apply_rule_schema_e4(&mut working, rule)?;
                            vsteps.push(VStep::Validate {
                                node_id: node_id.as_uuid(),
                                rule_ordinal,
                                predicate: predicate.clone(),
                                severity: *severity,
                                message: stored_message,
                            });
                        }
                        Rule::Deduplicate { keys } => {
                            flush_transform(&mut vsteps, &mut transform_group);
                            if keys.len() > MAX_DEDUP_KEY_COLUMNS {
                                return Err(EngineError::BoundExceeded(
                                    "dedup key column count exceeds MAX_DEDUP_KEY_COLUMNS",
                                ));
                            }
                            let mut key_types = Vec::with_capacity(keys.len());
                            let mut static_max = 0usize;
                            let mut all_fixed_width = true;
                            for key in keys {
                                let field = working
                                    .field(*key)
                                    .ok_or(EngineError::UnknownColumn(*key))?;
                                reject_paused_key_type(&field.data_type)?;
                                match static_component_max(&field.data_type) {
                                    Some(length) => {
                                        static_max += length;
                                    }
                                    None => {
                                        all_fixed_width = false;
                                    }
                                }
                                key_types.push(field.data_type.clone());
                            }
                            if all_fixed_width && static_max > MAX_DEDUP_KEY_BYTES {
                                return Err(EngineError::BoundExceeded(
                                    "fixed-width dedup key exceeds MAX_DEDUP_KEY_BYTES",
                                ));
                            }
                            apply_rule_schema_e4(&mut working, rule)?;
                            vsteps.push(VStep::Deduplicate {
                                node_id: node_id.as_uuid(),
                                rule_ordinal,
                                keys: keys.clone(),
                                key_types,
                            });
                        }
                        Rule::FilterRows { predicate } => {
                            flush_transform(&mut vsteps, &mut transform_group);
                            crate::typing::require_boolean_in(predicate, &working)?;
                            vsteps.push(VStep::FilterRows {
                                predicate: predicate.clone(),
                            });
                        }
                        other => {
                            transform_group.push(other.clone());
                            working = crate::preflight::apply_rule_schema(working, other, true)?;
                        }
                    }
                }
                flush_transform(&mut vsteps, &mut transform_group);
            }
            _ => {
                return Err(EngineError::Internal(
                    "unexpected plan node kind in verification preflight",
                ));
            }
        }
    }

    debug_assert_eq!(working, prepared.materialize_schema);
    Ok(VerificationPlan {
        vsteps: StdArc::new(vsteps),
        scan_boundary,
    })
}

fn flush_transform(vsteps: &mut Vec<VStep>, group: &mut Vec<Rule>) {
    if !group.is_empty() {
        vsteps.push(VStep::TransformRules {
            rules: std::mem::take(group),
        });
    }
}

fn apply_rule_schema_e4(
    schema: &mut stillflow_core::LogicalSchema,
    rule: &Rule,
) -> Result<(), EngineError> {
    // Validate and Deduplicate never change the working schema; this
    // mirrors the bypassing arm of the shared propagation (contract 6.1).
    let _ = (schema, rule);
    Ok(())
}

// ---------------------------------------------------------------------------
// Frozen single conversion site (contract 3.7 / 10.8)
// ---------------------------------------------------------------------------

/// THE frozen E4-S2 storage-error arm of `materialize_verification`: every
/// typed storage limit — including
/// `StorageError::DedupIndexLimitExceeded { resource: "page", … }` — becomes
/// terminal `EngineError::BoundExceeded` here and nowhere else. Everything
/// else keeps the plain storage wrapper.
pub(crate) fn map_verification_storage_error(error: StorageError) -> EngineError {
    match error {
        StorageError::EnvelopeLimitExceeded { .. }
        | StorageError::PartitionLimitExceeded { .. }
        | StorageError::RowLimitExceeded { .. }
        | StorageError::StoredByteLimitExceeded { .. }
        | StorageError::ArtifactRowLimitExceeded { .. }
        | StorageError::ArtifactByteLimitExceeded { .. }
        | StorageError::ArtifactPartitionLimitExceeded { .. }
        | StorageError::DedupKeyLimitExceeded { .. }
        | StorageError::DedupIndexLimitExceeded { .. } => {
            EngineError::BoundExceeded("verification storage limit exceeded")
        }
        other => EngineError::from_storage(other),
    }
}

use stillflow_storage::StorageError;

// ---------------------------------------------------------------------------
// Verification memory law (contract 12.1 / 12.2)
// ---------------------------------------------------------------------------

/// Six bounded live payloads plus the operator-state sum of the
/// verification path. Slot caps: four `MAX_BATCH_BYTES` columnar slots
/// (connector envelope, Polars working set, accepted remainder, rejected
/// remainder), two `REPORT_PACK_BYTES` report slots (validation report =
/// rule-summary + findings buffers, dedup report = rule-summary + finding
/// buffers), routing metadata ≤ [`VERIFICATION_MAX_ROUTING_STATE_BYTES`],
/// and the 5 MiB operator sum (compiled plan + FFI scratch + routing +
/// configured dedup page cache).
#[derive(Default)]
pub(crate) struct VerificationMemory {
    envelope_bytes: usize,
    polars_bytes: usize,
    accepted_remainder_bytes: usize,
    rejected_remainder_bytes: usize,
    validation_report_bytes: usize,
    dedup_report_bytes: usize,
    routing_bytes: usize,
    slots: [bool; 6],
    #[allow(dead_code)] // recorded for the 12.2 operator-state audit trail
    compiled_plan_bytes: usize,
    live_payloads: u8,
}

const DEDUP_CACHE_CONFIGURED_BYTES: usize = 512 * 1024;
const FFI_SCRATCH_BUDGET_BYTES: usize = 1024 * 1024;

impl VerificationMemory {
    const SLOT_ENVELOPE: usize = 0;
    const SLOT_POLARS: usize = 1;
    const SLOT_ACCEPTED: usize = 2;
    const SLOT_REJECTED: usize = 3;
    const SLOT_VALIDATION: usize = 4;
    const SLOT_DEDUP: usize = 5;

    /// Six-slot admission law (contract 12.1): a slot becomes live on its
    /// first buffered byte and dies when emptied; the six-slot ceiling is
    /// exact, and the aggregate columnar peak never exceeds 265 MiB.
    fn admit(
        slots: &mut [bool; 6],
        live: &mut u8,
        slot: usize,
        current: usize,
        incoming: usize,
        other_peak: usize,
        cap: usize,
    ) -> Result<usize, EngineError> {
        if !slots[slot] {
            slots[slot] = true;
            *live += 1;
            if *live > VERIFICATION_MAX_LIVE_COLUMNAR_PAYLOADS {
                return Err(EngineError::Internal(
                    "verification path exceeded six live payloads",
                ));
            }
        }
        let total = current.saturating_add(incoming);
        if total > cap {
            return Err(EngineError::BoundExceeded(
                "verification payload exceeds its bounded slot",
            ));
        }
        if other_peak + total > VERIFICATION_MAX_ENGINE_PEAK_BYTES {
            return Err(EngineError::BoundExceeded(
                "verification engine peak exceeds 265 MiB",
            ));
        }
        Ok(total)
    }

    fn release(slots: &mut [bool; 6], live: &mut u8, slot: usize, remaining: &mut usize) {
        // Callers zero the counter before releasing the slot.
        if *remaining == 0 && slots[slot] {
            slots[slot] = false;
            *live = live.saturating_sub(1);
        }
    }

    fn five_slot_peak_excluding(&self, excluded: usize) -> usize {
        let mut peak = 0usize;
        for (index, value) in [
            self.envelope_bytes,
            self.polars_bytes,
            self.accepted_remainder_bytes,
            self.rejected_remainder_bytes,
            self.validation_report_bytes,
            self.dedup_report_bytes,
        ]
        .into_iter()
        .enumerate()
        {
            if index != excluded {
                peak += value;
            }
        }
        peak
    }

    pub(crate) fn hold_envelope(&mut self, bytes: usize) -> Result<(), EngineError> {
        let other = self.five_slot_peak_excluding(Self::SLOT_ENVELOPE);
        self.envelope_bytes = Self::admit(
            &mut self.slots,
            &mut self.live_payloads,
            Self::SLOT_ENVELOPE,
            self.envelope_bytes,
            bytes,
            other,
            stillflow_core::MAX_BATCH_BYTES,
        )?;
        Ok(())
    }

    pub(crate) fn drop_envelope(&mut self) -> Result<(), EngineError> {
        // mem::take zeroes the counter; `release` observes the zeroed slot
        // counter and kills the slot (contract 12.1: a slot dies when
        // emptied).
        std::mem::take(&mut self.envelope_bytes);
        Self::release(
            &mut self.slots,
            &mut self.live_payloads,
            Self::SLOT_ENVELOPE,
            &mut self.envelope_bytes,
        );
        Ok(())
    }

    pub(crate) fn hold_polars(&mut self, bytes: usize) -> Result<(), EngineError> {
        let other = self.five_slot_peak_excluding(Self::SLOT_POLARS);
        self.polars_bytes = Self::admit(
            &mut self.slots,
            &mut self.live_payloads,
            Self::SLOT_POLARS,
            self.polars_bytes,
            bytes,
            other,
            stillflow_core::MAX_BATCH_BYTES,
        )?;
        Ok(())
    }

    pub(crate) fn drop_polars(&mut self) -> Result<(), EngineError> {
        std::mem::take(&mut self.polars_bytes);
        Self::release(
            &mut self.slots,
            &mut self.live_payloads,
            Self::SLOT_POLARS,
            &mut self.polars_bytes,
        );
        Ok(())
    }

    pub(crate) fn swap_envelope(&mut self, incoming: usize) -> Result<(), EngineError> {
        std::mem::take(&mut self.envelope_bytes);
        Self::release(
            &mut self.slots,
            &mut self.live_payloads,
            Self::SLOT_ENVELOPE,
            &mut self.envelope_bytes,
        );
        self.hold_envelope(incoming)
    }

    pub(crate) fn hold_accepted_remainder(&mut self, bytes: usize) -> Result<(), EngineError> {
        let other = self.five_slot_peak_excluding(Self::SLOT_ACCEPTED);
        self.accepted_remainder_bytes = Self::admit(
            &mut self.slots,
            &mut self.live_payloads,
            Self::SLOT_ACCEPTED,
            self.accepted_remainder_bytes,
            bytes,
            other,
            stillflow_core::MAX_BATCH_BYTES,
        )?;
        Ok(())
    }

    pub(crate) fn release_accepted_remainder(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.accepted_remainder_bytes = self.accepted_remainder_bytes.saturating_sub(bytes);
        if self.accepted_remainder_bytes == 0 && self.slots[Self::SLOT_ACCEPTED] {
            self.slots[Self::SLOT_ACCEPTED] = false;
            self.live_payloads -= 1;
        }
        Ok(())
    }

    pub(crate) fn hold_rejected_remainder(&mut self, bytes: usize) -> Result<(), EngineError> {
        let other = self.five_slot_peak_excluding(Self::SLOT_REJECTED);
        self.rejected_remainder_bytes = Self::admit(
            &mut self.slots,
            &mut self.live_payloads,
            Self::SLOT_REJECTED,
            self.rejected_remainder_bytes,
            bytes,
            other,
            stillflow_core::MAX_BATCH_BYTES,
        )?;
        Ok(())
    }

    pub(crate) fn release_rejected_remainder(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.rejected_remainder_bytes = self.rejected_remainder_bytes.saturating_sub(bytes);
        if self.rejected_remainder_bytes == 0 && self.slots[Self::SLOT_REJECTED] {
            self.slots[Self::SLOT_REJECTED] = false;
            self.live_payloads -= 1;
        }
        Ok(())
    }

    pub(crate) fn hold_validation_report(&mut self, bytes: usize) -> Result<(), EngineError> {
        let other = self.five_slot_peak_excluding(Self::SLOT_VALIDATION);
        self.validation_report_bytes = Self::admit(
            &mut self.slots,
            &mut self.live_payloads,
            Self::SLOT_VALIDATION,
            self.validation_report_bytes,
            bytes,
            other,
            stillflow_storage::artifact::REPORT_PACK_BYTES,
        )?;
        Ok(())
    }

    pub(crate) fn release_validation_report(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.validation_report_bytes = self.validation_report_bytes.saturating_sub(bytes);
        if self.validation_report_bytes == 0 && self.slots[Self::SLOT_VALIDATION] {
            self.slots[Self::SLOT_VALIDATION] = false;
            self.live_payloads -= 1;
        }
        Ok(())
    }

    pub(crate) fn hold_dedup_report(&mut self, bytes: usize) -> Result<(), EngineError> {
        let other = self.five_slot_peak_excluding(Self::SLOT_DEDUP);
        self.dedup_report_bytes = Self::admit(
            &mut self.slots,
            &mut self.live_payloads,
            Self::SLOT_DEDUP,
            self.dedup_report_bytes,
            bytes,
            other,
            stillflow_storage::artifact::REPORT_PACK_BYTES,
        )?;
        Ok(())
    }

    pub(crate) fn release_dedup_report(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.dedup_report_bytes = self.dedup_report_bytes.saturating_sub(bytes);
        if self.dedup_report_bytes == 0 && self.slots[Self::SLOT_DEDUP] {
            self.slots[Self::SLOT_DEDUP] = false;
            self.live_payloads -= 1;
        }
        Ok(())
    }

    #[allow(dead_code)] // routing-budget hook kept for V14 instrumentation
    pub(crate) fn hold_routing(&mut self, bytes: usize) -> Result<(), EngineError> {
        let projected = self.routing_bytes.saturating_add(bytes);
        if projected > VERIFICATION_MAX_ROUTING_STATE_BYTES {
            return Err(EngineError::BoundExceeded(
                "verification routing state exceeds 512 KiB",
            ));
        }
        self.routing_bytes = projected;
        Ok(())
    }

    /// Operator-state law: compiled plan + FFI budget + routing state +
    /// configured dedup cache <= 5 MiB (contract 12.2).
    pub(crate) fn check_operator_state(
        &self,
        compiled_plan_bytes: usize,
    ) -> Result<(), EngineError> {
        let operator_sum = compiled_plan_bytes
            + FFI_SCRATCH_BUDGET_BYTES
            + self.routing_bytes
            + DEDUP_CACHE_CONFIGURED_BYTES;
        if operator_sum > crate::MAX_OPERATOR_STATE_BYTES {
            return Err(EngineError::BoundExceeded(
                "verification operator state exceeds 5 MiB",
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Deterministic report packing (contract 12.1: fixed 1,024-row packs; 15.2:
// boundaries independent of connector partitioning) and rejected payloads.
// ---------------------------------------------------------------------------

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, StringBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
use stillflow_storage::artifact::{
    dedup_rule_summary_section_schema, duplicate_finding_section_schema,
    validation_finding_section_schema, validation_rule_summary_section_schema, REPORT_PACK_ROWS,
};

fn hex32(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push(char::from_digit(u32::from(byte >> 4), 16).expect("hex digit"));
        out.push(char::from_digit(u32::from(byte & 0x0F), 16).expect("hex digit"));
    }
    out
}

/// Copies `indices` rows of `array` into `builder`. The builder is created
/// from the same physical data type as the array, so every downcast is
/// schema-derived rather than user-dependent.
fn append_rows_to_builder(
    builder: &mut dyn ArrayBuilder,
    array: &dyn Array,
    indices: &[usize],
) -> Result<(), EngineError> {
    let mismatch =
        || EngineError::Internal("rejected payload column does not match its logical type");
    macro_rules! primitive {
        ($array_ty:ty, $builder_ty:ty) => {{
            let typed = array
                .as_any()
                .downcast_ref::<$array_ty>()
                .ok_or_else(mismatch)?;
            let out = builder
                .as_any_mut()
                .downcast_mut::<$builder_ty>()
                .ok_or_else(mismatch)?;
            for &index in indices {
                if array.is_null(index) {
                    out.append_null();
                } else {
                    out.append_value(typed.value(index));
                }
            }
        }};
    }
    match array.data_type() {
        DataType::Null => {
            let out = builder
                .as_any_mut()
                .downcast_mut::<arrow_array::builder::NullBuilder>()
                .ok_or_else(mismatch)?;
            out.append_nulls(indices.len());
        }
        DataType::Boolean => primitive!(BooleanArray, BooleanBuilder),
        DataType::Int8 => primitive!(Int8Array, Int8Builder),
        DataType::Int16 => primitive!(Int16Array, Int16Builder),
        DataType::Int32 => primitive!(Int32Array, Int32Builder),
        DataType::Int64 => primitive!(Int64Array, Int64Builder),
        DataType::UInt8 => primitive!(UInt8Array, UInt8Builder),
        DataType::UInt16 => primitive!(UInt16Array, UInt16Builder),
        DataType::UInt32 => primitive!(UInt32Array, UInt32Builder),
        DataType::UInt64 => primitive!(UInt64Array, UInt64Builder),
        DataType::Float32 => primitive!(Float32Array, Float32Builder),
        DataType::Float64 => primitive!(Float64Array, Float64Builder),
        DataType::Date32 => primitive!(Date32Array, Date32Builder),
        DataType::Timestamp(ArrowTimeUnit::Millisecond, _) => {
            primitive!(
                TimestampMillisecondArray,
                arrow_array::builder::TimestampMillisecondBuilder
            )
        }
        DataType::Timestamp(ArrowTimeUnit::Microsecond, _) => {
            primitive!(
                TimestampMicrosecondArray,
                arrow_array::builder::TimestampMicrosecondBuilder
            )
        }
        DataType::Timestamp(ArrowTimeUnit::Nanosecond, _) => {
            primitive!(
                TimestampNanosecondArray,
                arrow_array::builder::TimestampNanosecondBuilder
            )
        }
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(mismatch)?;
            let out = builder
                .as_any_mut()
                .downcast_mut::<arrow_array::builder::StringBuilder>()
                .ok_or_else(mismatch)?;
            for &index in indices {
                if array.is_null(index) {
                    out.append_null();
                } else {
                    out.append_value(typed.value(index));
                }
            }
        }
        DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(mismatch)?;
            let out = builder
                .as_any_mut()
                .downcast_mut::<arrow_array::builder::BinaryBuilder>()
                .ok_or_else(mismatch)?;
            for &index in indices {
                if array.is_null(index) {
                    out.append_null();
                } else {
                    out.append_value(typed.value(index));
                }
            }
        }
        _ => {
            return Err(EngineError::Internal(
                "unsupported rejected payload column type",
            ))
        }
    }
    Ok(())
}

/// Shared per-run provenance string prefix stamped into every report and
/// rejected row (contract 8.3-8.5 / 10.5).
/// Conservative per-row byte estimate for fixed-shape report rows; the
/// message column is capped at 1 KiB by preflight, so the estimate is an
/// upper bound by construction.
const REPORT_ROW_ESTIMATE_BYTES: usize = 2 * 1024;

#[derive(Debug, Clone, Copy)]
struct ValidationTally {
    evaluated: u64,
    pass: u64,
    fail: u64,
    warning: u64,
    error: u64,
    null_outcomes: u64,
    false_outcomes: u64,
}

impl ValidationTally {
    fn new() -> Self {
        Self {
            evaluated: 0,
            pass: 0,
            fail: 0,
            warning: 0,
            error: 0,
            null_outcomes: 0,
            false_outcomes: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct ValSummaryRow {
    node_id: Uuid,
    rule_ordinal: u32,
    message: String,
    tally: ValidationTally,
}

#[derive(Debug, Clone)]
struct ValFindingRow {
    ordinal: u64,
    node_id: Uuid,
    rule_ordinal: u32,
    severity: &'static str,
    outcome: &'static str,
}

#[derive(Debug, Clone)]
struct DedupSummaryRow {
    node_id: Uuid,
    rule_ordinal: u32,
    key_column_count: u32,
    evaluated: u64,
    unique: u64,
    duplicates: u64,
}

#[derive(Debug, Clone)]
struct DupFindingRow {
    ordinal: u64,
    first_ordinal: u64,
    node_id: Uuid,
    rule_ordinal: u32,
    key_column_count: u32,
    encoded_len: u32,
}

struct RejectedEntry {
    payload: RecordBatch,
    kind: &'static str,
    node_id: Uuid,
    rule_ordinal: u32,
    ordinal: u64,
    bytes: usize,
}

// ---------------------------------------------------------------------------
// Orchestration (contract section 10.1 publication sequence)
// ---------------------------------------------------------------------------

use stillflow_core::{BatchEnvelope, BatchEnvelopeFactory, MAX_BATCH_ROWS};
use stillflow_storage::{
    DedupIndex, DedupInsert, SnapshotDraft, VerificationBundle, VerificationBundleDraft,
    VerificationBundleWriter, MAX_SNAPSHOT_ROWS,
};

/// One verification run's mutable routing state.
struct VerificationRun {
    vplan: VerificationPlan,
    flush: FlushInputs,
    scan_output_logical: stillflow_core::LogicalSchema,
    scan_output_arrow: arrow_schema::SchemaRef,
    scan_output_fields: usize,
    materialize_logical: stillflow_core::LogicalSchema,
    materialize_arrow: arrow_schema::SchemaRef,
    batch_size: usize,
    next_ordinal: u64,
    memory: VerificationMemory,
    val_summaries: Vec<ValSummaryRow>,
    val_summary_index: std::collections::BTreeMap<(u128, u32), usize>,
    val_findings: Vec<ValFindingRow>,
    dedup_summaries: Vec<DedupSummaryRow>,
    dedup_summary_index: std::collections::BTreeMap<(u128, u32), usize>,
    dup_findings: Vec<DupFindingRow>,
    rejected: Vec<RejectedEntry>,
    accepted_batches: Vec<RecordBatch>,
    accepted_pending_rows: usize,
    accepted_buffered_bytes: usize,
    accepted_emitted_sequences: u64,
    terminal_rejections: u64,
    rejected_authorized: Option<Uuid>,
    scan_output_rejected_arrow: arrow_schema::SchemaRef,
}

impl VerificationRun {
    fn next_ordinal(&mut self) -> Result<u64, EngineError> {
        let ordinal = self.next_ordinal;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or(EngineError::BoundExceeded(
                "source row ordinal space is exhausted",
            ))?;
        if ordinal >= MAX_SNAPSHOT_ROWS {
            return Err(EngineError::BoundExceeded(
                "logical Scan output exceeds MAX_SNAPSHOT_ROWS",
            ));
        }
        Ok(ordinal)
    }

    fn hold_validation_row(&mut self) -> Result<(), EngineError> {
        self.memory
            .hold_validation_report(REPORT_ROW_ESTIMATE_BYTES)
    }

    fn release_validation_rows(&mut self, count: usize) -> Result<(), EngineError> {
        for _ in 0..count {
            self.memory
                .release_validation_report(REPORT_ROW_ESTIMATE_BYTES)?;
        }
        Ok(())
    }

    fn hold_dedup_report_row(&mut self) -> Result<(), EngineError> {
        self.memory.hold_dedup_report(REPORT_ROW_ESTIMATE_BYTES)
    }

    fn release_dedup_report_rows(&mut self, count: usize) -> Result<(), EngineError> {
        for _ in 0..count {
            self.memory
                .release_dedup_report(REPORT_ROW_ESTIMATE_BYTES)?;
        }
        Ok(())
    }
}

/// Creates the concrete builder for one physical column type. Mirrors the
/// frozen `logical_type_to_arrow` mapping of stillflow-core; a mismatch is
/// an engine bug, not user input.
fn new_builder_for(
    data_type: &DataType,
    capacity: usize,
) -> Result<Box<dyn ArrayBuilder>, EngineError> {
    let mismatch = || EngineError::Internal("unsupported column type for verification output");
    Ok(match data_type {
        DataType::Null => Box::new(arrow_array::builder::NullBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 4 * capacity)),
        DataType::Binary => Box::new(arrow_array::builder::BinaryBuilder::with_capacity(
            capacity,
            4 * capacity,
        )),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Timestamp(ArrowTimeUnit::Millisecond, tz) => {
            let mut builder =
                arrow_array::builder::TimestampMillisecondBuilder::with_capacity(capacity);
            if let Some(tz) = tz {
                builder = builder.with_timezone(StdArc::clone(tz));
            }
            Box::new(builder)
        }
        DataType::Timestamp(ArrowTimeUnit::Microsecond, tz) => {
            let mut builder =
                arrow_array::builder::TimestampMicrosecondBuilder::with_capacity(capacity);
            if let Some(tz) = tz {
                builder = builder.with_timezone(StdArc::clone(tz));
            }
            Box::new(builder)
        }
        DataType::Timestamp(ArrowTimeUnit::Nanosecond, tz) => {
            let mut builder =
                arrow_array::builder::TimestampNanosecondBuilder::with_capacity(capacity);
            if let Some(tz) = tz {
                builder = builder.with_timezone(StdArc::clone(tz));
            }
            Box::new(builder)
        }
        _ => return Err(mismatch()),
    })
}

/// Column builders for one target Arrow schema.
struct ColumnBuilders {
    builders: Vec<Box<dyn ArrayBuilder>>,
}

impl ColumnBuilders {
    fn new(schema: &arrow_schema::SchemaRef) -> Result<Self, EngineError> {
        let builders = schema
            .fields()
            .iter()
            .map(|field| new_builder_for(field.data_type(), REPORT_PACK_ROWS))
            .collect::<Result<Vec<_>, EngineError>>()?;
        Ok(Self { builders })
    }

    fn append_rows(&mut self, batch: &RecordBatch, indices: &[usize]) -> Result<(), EngineError> {
        if batch.num_columns() != self.builders.len() {
            return Err(EngineError::Internal("verification row width mismatch"));
        }
        for (builder, column) in self.builders.iter_mut().zip(batch.columns()) {
            append_rows_to_builder(builder.as_mut(), column.as_ref(), indices)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Vec<StdArc<dyn Array>> {
        self.builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect()
    }
}

use std::sync::Arc as StdArc;

/// Emits deterministic fixed-size packs for one artifact section.
struct PackSink {
    factory: BatchEnvelopeFactory,
    next_sequence: u64,
}

impl PackSink {
    fn new(
        schema: stillflow_core::LogicalSchema,
        source_asset_id: Uuid,
    ) -> Result<Self, EngineError> {
        let factory = BatchEnvelopeFactory::try_new(StdArc::new(schema), source_asset_id)
            .map_err(|_| EngineError::Internal("verification section factory build failed"))?;
        Ok(Self {
            factory,
            next_sequence: 0,
        })
    }

    fn emit(
        &mut self,
        context: &RequestContext,
        batch: RecordBatch,
        append: impl FnOnce(&BatchEnvelope) -> Result<(), stillflow_storage::StorageError>,
    ) -> Result<(), EngineError> {
        context.ensure_active().map_err(map_context_error)?;
        let envelope = self
            .factory
            .try_build(self.next_sequence, batch)
            .map_err(|_| EngineError::Internal("verification section envelope build failed"))?;
        self.next_sequence += 1;
        append(&envelope).map_err(map_verification_storage_error)
    }
}

fn severity_literal(severity: ValidationSeverity) -> &'static str {
    match severity {
        ValidationSeverity::Warning => "warning",
        ValidationSeverity::Error => "error",
    }
}

/// Builds one report batch from per-column closures over the frozen
/// section schema. Column order is the schema's declaration order.
#[allow(clippy::type_complexity)]
fn build_report_batch(
    _logical: &stillflow_core::LogicalSchema,
    arrow: &arrow_schema::SchemaRef,
    fill: &mut dyn FnMut(&mut [Box<dyn ArrayBuilder>]) -> Result<(), EngineError>,
) -> Result<RecordBatch, EngineError> {
    let mut builders = ColumnBuilders::new(arrow)?;
    fill(&mut builders.builders)?;
    let arrays = builders.finish();
    RecordBatch::try_new(arrow.clone(), arrays)
        .map_err(|_| EngineError::Internal("report section batch build failed"))
}

#[derive(Clone)]
struct FlushInputs {
    input_kind: &'static str,
    input_id: String,
    input_version_digest: String,
    plan_fingerprint_hex: String,
    canonical_plan_digest_hex: String,
}

impl FlushInputs {
    fn new(
        asset_id: Uuid,
        logical_input: &LogicalInputRef,
        fingerprint: &[u8; 32],
        digest: &[u8; 32],
    ) -> Self {
        Self {
            input_kind: "asset",
            input_id: asset_id.to_string(),
            input_version_digest: hex32(&logical_input.version_digest),
            plan_fingerprint_hex: hex32(fingerprint),
            canonical_plan_digest_hex: hex32(digest),
        }
    }
}

fn fill_text(
    builder: &mut Box<dyn ArrayBuilder>,
    value: &str,
    rows: usize,
) -> Result<(), EngineError> {
    let out = builder
        .as_any_mut()
        .downcast_mut::<StringBuilder>()
        .ok_or(EngineError::Internal("report column type drift"))?;
    for _ in 0..rows {
        out.append_value(value);
    }
    Ok(())
}

fn fill_uuids(builder: &mut Box<dyn ArrayBuilder>, values: &[Uuid]) -> Result<(), EngineError> {
    let out = builder
        .as_any_mut()
        .downcast_mut::<StringBuilder>()
        .ok_or(EngineError::Internal("report column type drift"))?;
    for value in values {
        out.append_value(value.to_string());
    }
    Ok(())
}

fn fill_u32(builder: &mut Box<dyn ArrayBuilder>, values: &[u32]) -> Result<(), EngineError> {
    let out = builder
        .as_any_mut()
        .downcast_mut::<UInt32Builder>()
        .ok_or(EngineError::Internal("report column type drift"))?;
    for value in values {
        out.append_value(*value);
    }
    Ok(())
}

fn fill_u64(builder: &mut Box<dyn ArrayBuilder>, values: &[u64]) -> Result<(), EngineError> {
    let out = builder
        .as_any_mut()
        .downcast_mut::<UInt64Builder>()
        .ok_or(EngineError::Internal("report column type drift"))?;
    for value in values {
        out.append_value(*value);
    }
    Ok(())
}

fn flush_validation_findings(
    run: &mut VerificationRun,
    sink: &mut PackSink,
    writer: &mut VerificationBundleWriter,
    inputs: FlushInputs,
    context: &RequestContext,
) -> Result<(), EngineError> {
    if run.val_findings.is_empty() {
        return Ok(());
    }
    let count = run.val_findings.len();
    let schema = validation_finding_section_schema();
    let arrow = stillflow_core::logical_schema_to_arrow(&schema)
        .map_err(|_| EngineError::Internal("finding arrow schema failed"))?;
    let node_ids: Vec<Uuid> = run.val_findings.iter().map(|row| row.node_id).collect();
    let rule_ordinals: Vec<u32> = run
        .val_findings
        .iter()
        .map(|row| row.rule_ordinal)
        .collect();
    let source_ordinals: Vec<u64> = run.val_findings.iter().map(|row| row.ordinal).collect();
    let severities: Vec<&'static str> = run.val_findings.iter().map(|row| row.severity).collect();
    let outcomes: Vec<&'static str> = run.val_findings.iter().map(|row| row.outcome).collect();
    let batch = build_report_batch(&schema, &arrow, &mut |builders| {
        // Frozen column order (artifact.rs): kind, id, version digest,
        // SOURCE_ROW_ORDINAL, plan fingerprint, canonical digest, node,
        // rule ordinal, severity, outcome.
        fill_text(&mut builders[0], inputs.input_kind, count)?;
        fill_text(&mut builders[1], &inputs.input_id, count)?;
        fill_text(&mut builders[2], &inputs.input_version_digest, count)?;
        fill_u64(&mut builders[3], &source_ordinals)?;
        fill_text(&mut builders[4], &inputs.plan_fingerprint_hex, count)?;
        fill_text(&mut builders[5], &inputs.canonical_plan_digest_hex, count)?;
        fill_uuids(&mut builders[6], &node_ids)?;
        fill_u32(&mut builders[7], &rule_ordinals)?;
        for index in 0..count {
            fill_text(&mut builders[8], severities[index], 1)?;
            fill_text(&mut builders[9], outcomes[index], 1)?;
        }
        Ok(())
    })?;
    run.release_validation_rows(count)?;
    sink.emit(context, batch, |envelope| {
        writer.append_validation_findings(envelope)
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn flush_validation_rule_summaries(
    run: &mut VerificationRun,
    sink: &mut PackSink,
    writer: &mut VerificationBundleWriter,
    inputs: FlushInputs,
    context: &RequestContext,
) -> Result<(), EngineError> {
    while !run.val_summaries.is_empty() {
        let take = run.val_summaries.len().min(REPORT_PACK_ROWS);
        let chunk: Vec<ValSummaryRow> = run.val_summaries.drain(..take).collect();
        let count = chunk.len();
        let schema = validation_rule_summary_section_schema();
        let arrow = stillflow_core::logical_schema_to_arrow(&schema)
            .map_err(|_| EngineError::Internal("summary arrow schema failed"))?;
        let node_ids: Vec<Uuid> = chunk.iter().map(|row| row.node_id).collect();
        let rule_ordinals: Vec<u32> = chunk.iter().map(|row| row.rule_ordinal).collect();
        let evaluated: Vec<u64> = chunk.iter().map(|row| row.tally.evaluated).collect();
        let pass: Vec<u64> = chunk.iter().map(|row| row.tally.pass).collect();
        let fail: Vec<u64> = chunk.iter().map(|row| row.tally.fail).collect();
        let warning: Vec<u64> = chunk.iter().map(|row| row.tally.warning).collect();
        let error: Vec<u64> = chunk.iter().map(|row| row.tally.error).collect();
        let nulls: Vec<u64> = chunk.iter().map(|row| row.tally.null_outcomes).collect();
        let false_counts: Vec<u64> = chunk.iter().map(|row| row.tally.false_outcomes).collect();
        let batch = build_report_batch(&schema, &arrow, &mut |builders| {
            fill_text(&mut builders[0], inputs.input_kind, count)?;
            fill_text(&mut builders[1], &inputs.input_id, count)?;
            fill_text(&mut builders[2], &inputs.input_version_digest, count)?;
            fill_text(&mut builders[3], &inputs.plan_fingerprint_hex, count)?;
            fill_text(&mut builders[4], &inputs.canonical_plan_digest_hex, count)?;
            fill_uuids(&mut builders[5], &node_ids)?;
            fill_u32(&mut builders[6], &rule_ordinals)?;
            for row in &chunk {
                fill_text(&mut builders[7], &row.message, 1)?;
            }
            fill_u64(&mut builders[8], &evaluated)?;
            fill_u64(&mut builders[9], &pass)?;
            fill_u64(&mut builders[10], &fail)?;
            fill_u64(&mut builders[11], &warning)?;
            fill_u64(&mut builders[12], &error)?;
            fill_u64(&mut builders[13], &nulls)?;
            fill_u64(&mut builders[14], &false_counts)?;
            Ok(())
        })?;
        sink.emit(context, batch, |envelope| {
            writer.append_validation_rule_summary(envelope)
        })?;
    }
    Ok(())
}

fn flush_duplicate_findings(
    run: &mut VerificationRun,
    sink: &mut PackSink,
    writer: &mut VerificationBundleWriter,
    inputs: FlushInputs,
    context: &RequestContext,
) -> Result<(), EngineError> {
    if run.dup_findings.is_empty() {
        return Ok(());
    }
    let count = run.dup_findings.len();
    let schema = duplicate_finding_section_schema();
    let arrow = stillflow_core::logical_schema_to_arrow(&schema)
        .map_err(|_| EngineError::Internal("duplicate finding arrow schema failed"))?;
    let node_ids: Vec<Uuid> = run.dup_findings.iter().map(|row| row.node_id).collect();
    let rule_ordinals: Vec<u32> = run
        .dup_findings
        .iter()
        .map(|row| row.rule_ordinal)
        .collect();
    let source_ordinals: Vec<u64> = run.dup_findings.iter().map(|row| row.ordinal).collect();
    let first_ordinals: Vec<u64> = run
        .dup_findings
        .iter()
        .map(|row| row.first_ordinal)
        .collect();
    let key_columns: Vec<u32> = run
        .dup_findings
        .iter()
        .map(|row| row.key_column_count)
        .collect();
    let encoded_lengths: Vec<u32> = run.dup_findings.iter().map(|row| row.encoded_len).collect();
    let batch = build_report_batch(&schema, &arrow, &mut |builders| {
        fill_text(&mut builders[0], inputs.input_kind, count)?;
        fill_text(&mut builders[1], &inputs.input_id, count)?;
        fill_text(&mut builders[2], &inputs.input_version_digest, count)?;
        fill_u64(&mut builders[3], &source_ordinals)?;
        fill_u64(&mut builders[4], &first_ordinals)?;
        fill_text(&mut builders[5], &inputs.plan_fingerprint_hex, count)?;
        fill_text(&mut builders[6], &inputs.canonical_plan_digest_hex, count)?;
        fill_uuids(&mut builders[7], &node_ids)?;
        fill_u32(&mut builders[8], &rule_ordinals)?;
        fill_u32(&mut builders[9], &key_columns)?;
        fill_u32(&mut builders[10], &encoded_lengths)?;
        Ok(())
    })?;
    run.release_dedup_report_rows(count)?;
    sink.emit(context, batch, |envelope| {
        writer.append_duplicate_findings(envelope)
    })?;
    Ok(())
}

fn flush_dedup_rule_summaries(
    run: &mut VerificationRun,
    sink: &mut PackSink,
    writer: &mut VerificationBundleWriter,
    inputs: FlushInputs,
    context: &RequestContext,
) -> Result<(), EngineError> {
    while !run.dedup_summaries.is_empty() {
        let take = run.dedup_summaries.len().min(REPORT_PACK_ROWS);
        let chunk: Vec<DedupSummaryRow> = run.dedup_summaries.drain(..take).collect();
        let count = chunk.len();
        let schema = dedup_rule_summary_section_schema();
        let arrow = stillflow_core::logical_schema_to_arrow(&schema)
            .map_err(|_| EngineError::Internal("dedup summary arrow schema failed"))?;
        let node_ids: Vec<Uuid> = chunk.iter().map(|row| row.node_id).collect();
        let rule_ordinals: Vec<u32> = chunk.iter().map(|row| row.rule_ordinal).collect();
        let key_columns: Vec<u32> = chunk.iter().map(|row| row.key_column_count).collect();
        let evaluated: Vec<u64> = chunk.iter().map(|row| row.evaluated).collect();
        let unique: Vec<u64> = chunk.iter().map(|row| row.unique).collect();
        let duplicates: Vec<u64> = chunk.iter().map(|row| row.duplicates).collect();
        let batch = build_report_batch(&schema, &arrow, &mut |builders| {
            fill_text(&mut builders[0], inputs.input_kind, count)?;
            fill_text(&mut builders[1], &inputs.input_id, count)?;
            fill_text(&mut builders[2], &inputs.input_version_digest, count)?;
            fill_text(&mut builders[3], &inputs.plan_fingerprint_hex, count)?;
            fill_text(&mut builders[4], &inputs.canonical_plan_digest_hex, count)?;
            fill_uuids(&mut builders[5], &node_ids)?;
            fill_u32(&mut builders[6], &rule_ordinals)?;
            fill_u32(&mut builders[7], &key_columns)?;
            fill_u64(&mut builders[8], &evaluated)?;
            fill_u64(&mut builders[9], &unique)?;
            fill_u64(&mut builders[10], &duplicates)?;
            Ok(())
        })?;
        sink.emit(context, batch, |envelope| {
            writer.append_dedup_rule_summary(envelope)
        })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn flush_rejected_rows(
    run: &mut VerificationRun,
    sink: &mut PackSink,
    writer: &mut VerificationBundleWriter,
    inputs: FlushInputs,
    context: &RequestContext,
) -> Result<(), EngineError> {
    while !run.rejected.is_empty() {
        let take = run.rejected.len().min(REPORT_PACK_ROWS);
        let chunk: Vec<RejectedEntry> = run.rejected.drain(..take).collect();
        let count = chunk.len();
        let source_field_count = run.scan_output_fields;
        let arrow = run.scan_output_rejected_arrow.clone();
        debug_assert_eq!(arrow.fields().len(), source_field_count + 9);
        let mut builders = ColumnBuilders::new(&arrow)?;
        let kinds: Vec<&'static str> = chunk.iter().map(|entry| entry.kind).collect();
        let node_ids: Vec<Uuid> = chunk.iter().map(|entry| entry.node_id).collect();
        let rule_ordinals: Vec<u32> = chunk.iter().map(|entry| entry.rule_ordinal).collect();
        let ordinals: Vec<u64> = chunk.iter().map(|entry| entry.ordinal).collect();
        for (index, builder) in builders.builders.iter_mut().enumerate() {
            if index < source_field_count {
                for entry in &chunk {
                    append_rows_to_builder(
                        builder.as_mut(),
                        entry.payload.column(index).as_ref(),
                        &[0],
                    )?;
                }
            } else {
                let control = index - source_field_count;
                match control {
                    0 => fill_text(builder, inputs.input_kind, count)?,
                    1 => fill_text(builder, &inputs.input_id, count)?,
                    2 => fill_text(builder, &inputs.input_version_digest, count)?,
                    3 => fill_u64(builder, &ordinals)?,
                    4 => {
                        let out = builder
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .ok_or(EngineError::Internal("rejected column type drift"))?;
                        for value in &kinds {
                            out.append_value(value);
                        }
                    }
                    5 => fill_text(builder, &inputs.plan_fingerprint_hex, count)?,
                    6 => fill_text(builder, &inputs.canonical_plan_digest_hex, count)?,
                    7 => fill_uuids(builder, &node_ids)?,
                    8 => fill_u32(builder, &rule_ordinals)?,
                    _ => return Err(EngineError::Internal("rejected control index overflow")),
                }
            }
        }
        let arrays = builders.finish();
        let batch = RecordBatch::try_new(arrow.clone(), arrays)
            .map_err(|_| EngineError::Internal("rejected batch build failed"))?;
        let released: usize = chunk.iter().map(|entry| entry.bytes).sum();
        sink.emit(context, batch, |envelope| {
            writer.append_rejected_rows(envelope)
        })?;
        memory_release_rejected(run, released)?;
    }
    Ok(())
}

fn memory_release_rejected(run: &mut VerificationRun, bytes: usize) -> Result<(), EngineError> {
    run.memory.release_rejected_remainder(bytes)
}

impl VerificationRun {
    fn hold_accepted(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.memory.hold_accepted_remainder(bytes)?;
        self.accepted_buffered_bytes += bytes;
        Ok(())
    }

    fn release_accepted(&mut self, bytes: usize) -> Result<(), EngineError> {
        self.memory.release_accepted_remainder(bytes)?;
        self.accepted_buffered_bytes = self.accepted_buffered_bytes.saturating_sub(bytes);
        Ok(())
    }
}

/// Extracts one canonical key component from the working Polars column at
/// `row` (contract 6.2/6.4). The declared working type drives the mapping;
/// any drift is an engine bug.
fn key_component<'a>(
    series: &'a polars::prelude::Series,
    row: usize,
    declared: &'a LogicalType,
) -> Result<KeyValue<'a>, EngineError> {
    use polars::prelude::AnyValue;
    let value = series
        .get(row)
        .map_err(|_| EngineError::Internal("dedup key read failed"))?;
    let mismatch =
        || EngineError::Internal("dedup key component does not match its declared working type");
    if matches!(value, AnyValue::Null) {
        return Ok(KeyValue::Null);
    }
    match (declared, value) {
        (LogicalType::Boolean, AnyValue::Boolean(inner)) => Ok(KeyValue::Boolean(inner)),
        (LogicalType::Int8, AnyValue::Int8(inner)) => Ok(KeyValue::Int8(inner)),
        (LogicalType::Int16, AnyValue::Int16(inner)) => Ok(KeyValue::Int16(inner)),
        (LogicalType::Int32, AnyValue::Int32(inner)) => Ok(KeyValue::Int32(inner)),
        (LogicalType::Int64, AnyValue::Int64(inner)) => Ok(KeyValue::Int64(inner)),
        (LogicalType::UInt8, AnyValue::UInt8(inner)) => Ok(KeyValue::UInt8(inner)),
        (LogicalType::UInt16, AnyValue::UInt16(inner)) => Ok(KeyValue::UInt16(inner)),
        (LogicalType::UInt32, AnyValue::UInt32(inner)) => Ok(KeyValue::UInt32(inner)),
        (LogicalType::UInt64, AnyValue::UInt64(inner)) => Ok(KeyValue::UInt64(inner)),
        (LogicalType::Float32, AnyValue::Float32(inner)) => Ok(KeyValue::Float32(inner)),
        (LogicalType::Float64, AnyValue::Float64(inner)) => Ok(KeyValue::Float64(inner)),
        (LogicalType::Utf8, AnyValue::String(inner)) => Ok(KeyValue::Utf8(inner)),
        (LogicalType::Utf8, AnyValue::StringOwned(inner)) => {
            Ok(KeyValue::Utf8Owned(inner.to_string()))
        }
        (LogicalType::Binary, AnyValue::Binary(inner)) => Ok(KeyValue::Binary(inner)),
        (LogicalType::Date32, AnyValue::Date(days)) => Ok(KeyValue::Date32(days)),
        (
            LogicalType::Timestamp { unit, timezone },
            AnyValue::Datetime(epoch, value_unit, value_timezone),
        ) => {
            let value_unit_core = match value_unit {
                polars::prelude::TimeUnit::Milliseconds => TimeUnit::Millisecond,
                polars::prelude::TimeUnit::Microseconds => TimeUnit::Microsecond,
                polars::prelude::TimeUnit::Nanoseconds => TimeUnit::Nanosecond,
            };
            if *unit != value_unit_core {
                return Err(mismatch());
            }
            match (timezone, value_timezone) {
                (None, None) | (Some(_), Some(_)) => Ok(KeyValue::Timestamp {
                    epoch,
                    unit: *unit,
                    timezone: timezone.as_deref(),
                }),
                _ => Err(mismatch()),
            }
        }
        _ => Err(mismatch()),
    }
}

/// Evaluates one Boolean predicate against the working frame and returns
/// the per-row keep-mask used by every row-dropping step. `null` and
/// `false` both drop (E2 Filter/FilterRows semantics; contract 5.3).
fn boolean_keep_mask(
    frame: &polars::prelude::DataFrame,
    schema: &stillflow_core::LogicalSchema,
    predicate: &Expr,
) -> Result<Vec<bool>, EngineError> {
    use polars::prelude::IntoLazy;
    let lowered = crate::lower::lower_expr(predicate, schema)?;
    let selected = frame
        .clone()
        .lazy()
        .select([lowered.alias("__sf_keep")])
        .collect()
        .map_err(|_| EngineError::TypeError("predicate evaluation failed"))?;
    let column = selected
        .column("__sf_keep")
        .map_err(|_| EngineError::Internal("predicate column missing"))?;
    let typed = column
        .as_materialized_series()
        .bool()
        .map_err(|_| EngineError::TypeError("predicate did not evaluate to Boolean"))?;
    Ok(typed
        .into_iter()
        .map(|value| matches!(value, Some(true)))
        .collect())
}

/// Compacts the frame to the surviving rows of `keep_mask`.
fn take_surviving(
    frame: &mut polars::prelude::DataFrame,
    keep_mask: &[bool],
) -> Result<(), EngineError> {
    use polars::prelude::{col, IntoLazy, IntoSeries, NewChunkedArray};
    let keep_column =
        polars::prelude::BooleanChunked::from_slice("keep".into(), keep_mask).into_series();
    let mut masked = frame.clone();
    masked
        .with_column(keep_column)
        .map_err(|_| EngineError::Internal("row compaction failed"))?;
    let filtered = masked
        .lazy()
        .filter(col("keep"))
        .collect()
        .map_err(|_| EngineError::Internal("row compaction failed"))?;
    let mut result = filtered;
    result
        .drop_in_place("keep")
        .map_err(|_| EngineError::Internal("row compaction failed"))?;
    *frame = result;
    Ok(())
}

/// Compacts the working frame, its ordinal sidecar, and per-row finding
/// counters to the surviving rows (contract 5.2/5.3 terminal routing).
fn compact_alive(
    frame: &mut polars::prelude::DataFrame,
    sidecar: &mut Vec<(usize, u64)>,
    findings_per_row: &mut Vec<u32>,
    keep_mask: &[bool],
) -> Result<(), EngineError> {
    if keep_mask.iter().all(|keep| *keep) {
        return Ok(());
    }
    take_surviving(frame, keep_mask)?;
    let mut next_sidecar = Vec::with_capacity(keep_mask.len());
    let mut next_caps = Vec::with_capacity(keep_mask.len());
    for (index, keep) in keep_mask.iter().enumerate() {
        if *keep {
            next_sidecar.push(sidecar[index]);
            next_caps.push(findings_per_row[index]);
        }
    }
    *sidecar = next_sidecar;
    *findings_per_row = next_caps;
    Ok(())
}

struct ReportSinks {
    val_summary: PackSink,
    val_finding: PackSink,
    rejected: PackSink,
    dedup_summary: PackSink,
    dup_finding: PackSink,
}

#[allow(clippy::too_many_arguments)]
fn flush_accepted_packs(
    run: &mut VerificationRun,
    factory: &BatchEnvelopeFactory,
    writer: &mut VerificationBundleWriter,
    context: &RequestContext,
) -> Result<(), EngineError> {
    while run.accepted_pending_rows >= run.batch_size {
        let mut needed = run.batch_size;
        let mut parts: Vec<RecordBatch> = Vec::new();
        while needed > 0 {
            let front = run
                .accepted_batches
                .first()
                .ok_or(EngineError::Internal("accepted packer underflow"))?;
            if front.num_rows() <= needed {
                let ready = run.accepted_batches.remove(0);
                needed -= ready.num_rows();
                run.accepted_pending_rows -= ready.num_rows();
                let bytes = ready.get_array_memory_size();
                run.release_accepted(bytes)?;
                parts.push(ready);
            } else {
                let head = front.slice(0, needed);
                run.accepted_batches[0] = front.slice(needed, front.num_rows() - needed);
                run.accepted_pending_rows -= needed;
                needed = 0;
                parts.push(head);
            }
        }
        let payload = if parts.len() == 1 {
            parts.remove(0)
        } else {
            // Deterministic multi-chunk pack: rebuild through typed builders
            // so no concat dependency is required.
            let mut builders = ColumnBuilders::new(factory.arrow_schema())?;
            for part in &parts {
                let indices: Vec<usize> = (0..part.num_rows()).collect();
                builders.append_rows(part, &indices)?;
            }
            let arrays = builders.finish();
            RecordBatch::try_new(factory.arrow_schema().clone(), arrays)
                .map_err(|_| EngineError::Internal("accepted pack rebuild failed"))?
        };
        context.ensure_active().map_err(map_context_error)?;
        let envelope = factory
            .try_build(run.accepted_emitted_sequences, payload)
            .map_err(|_| EngineError::Internal("accepted envelope build failed"))?;
        run.accepted_emitted_sequences += 1;
        writer
            .append_accepted(&envelope)
            .map_err(map_verification_storage_error)?;
    }
    Ok(())
}

fn finish_accepted(
    run: &mut VerificationRun,
    factory: &BatchEnvelopeFactory,
    writer: &mut VerificationBundleWriter,
    context: &RequestContext,
) -> Result<(), EngineError> {
    flush_accepted_packs(run, factory, writer, context)?;
    if run.accepted_pending_rows == 0 {
        return Ok(());
    }
    let mut parts = std::mem::take(&mut run.accepted_batches);
    let pending = run.accepted_pending_rows;
    run.accepted_pending_rows = 0;
    let payload = if parts.len() == 1 {
        let only = parts.pop().expect("checked non-empty");
        run.release_accepted(only.get_array_memory_size())?;
        only
    } else {
        let total_bytes: usize = parts
            .iter()
            .map(|batch| batch.get_array_memory_size())
            .sum();
        run.release_accepted(total_bytes)?;
        let mut builders = ColumnBuilders::new(factory.arrow_schema())?;
        for part in &parts {
            let indices: Vec<usize> = (0..part.num_rows()).collect();
            builders.append_rows(part, &indices)?;
        }
        let arrays = builders.finish();
        RecordBatch::try_new(factory.arrow_schema().clone(), arrays)
            .map_err(|_| EngineError::Internal("accepted tail rebuild failed"))?
    };
    debug_assert_eq!(payload.num_rows(), pending);
    context.ensure_active().map_err(map_context_error)?;
    let envelope = factory
        .try_build(run.accepted_emitted_sequences, payload)
        .map_err(|_| EngineError::Internal("accepted envelope build failed"))?;
    run.accepted_emitted_sequences += 1;
    writer
        .append_accepted(&envelope)
        .map_err(map_verification_storage_error)
}

/// Processes one connector envelope through the verification pipeline:
/// leading Scan-output steps, ordinal assignment, routing rules, accepted
/// remainder packing, and report/rejected emission (contract 5, 6, 10.3).
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
fn process_envelope(
    run: &mut VerificationRun,
    prepared: &super::preflight::PreparedPlan,
    expected_fingerprint: &stillflow_core::LogicalSchemaFingerprint,
    origin_envelope: BatchEnvelope,
    dedup: &DedupIndex,
    writer: &mut VerificationBundleWriter,
    sinks: &mut ReportSinks,
    accepted_factory: &BatchEnvelopeFactory,
    context: &RequestContext,
) -> Result<(), EngineError> {
    // Checkpoint 4: before lowering the envelope.
    context.ensure_active().map_err(map_context_error)?;
    let inputs = run.flush.clone();
    if origin_envelope.schema() != &prepared.expected_connector
        || origin_envelope.schema_fingerprint() != *expected_fingerprint
    {
        return Err(EngineError::SchemaDrift {
            sequence: origin_envelope.sequence(),
        });
    }
    run.memory.hold_envelope(origin_envelope.byte_count())?;
    let mut deferred: Vec<(String, stillflow_core::ScalarValue)> = Vec::new();
    {
        let _polars_phase = crate::memory::enter_phase(crate::memory::AllocatorPhase::Polars);
        let mut frame = crate::ffi::record_batch_to_dataframe(origin_envelope.payload())?;
        run.memory.hold_polars(frame.estimated_size())?;
        let mut working = prepared.expected_connector.clone();

        // Leading steps: projection (when not pushed down) and the
        // in-engine Scan predicate. Ordinals start only after both
        // (contract 5.1).
        for step in run.vplan.vsteps.iter().take(run.vplan.scan_boundary) {
            match step {
                VStep::Project { columns } => {
                    let compiled = [super::preflight::CompiledStep::Project {
                        columns: columns.clone(),
                    }];
                    let (next_frame, next_deferred) =
                        crate::lower::transform(frame, &working, &compiled, deferred)?;
                    deferred = next_deferred;
                    frame = next_frame;
                    working = crate::preflight::project_schema(&working, columns)?;
                }
                VStep::Filter { predicate } => {
                    let keep = boolean_keep_mask(&frame, &working, predicate)?;
                    if keep.iter().any(|keep| !keep) {
                        take_surviving(&mut frame, &keep)?;
                    }
                }
                _ => {
                    return Err(EngineError::Internal(
                        "non-leading verification step before the scan boundary",
                    ));
                }
            }
        }

        // Ordinal assignment at the logical Scan output boundary.
        let height = frame.height();
        let mut sidecar: Vec<(usize, u64)> = Vec::with_capacity(height);
        for _ in 0..height {
            let ordinal = run.next_ordinal()?;
            sidecar.push((sidecar.len(), ordinal));
        }

        // Retained Scan-output payload (the envelope slot's second life).
        let origin = crate::ffi::dataframe_to_record_batch(
            frame.clone(),
            &run.scan_output_logical,
            &run.scan_output_arrow,
            &deferred,
        )?;
        let origin_bytes = origin.get_array_memory_size();
        run.memory.drop_polars()?;
        run.memory.swap_envelope(origin_bytes)?;

        // Routing sub-slices rehydrate from the retained origin so the raw
        // envelope slot is the only input-side payload (contract 12.1).
        let total = origin.num_rows();
        let vsteps = StdArc::clone(&run.vplan.vsteps);
        let scan_boundary = run.vplan.scan_boundary;
        let mut offset = 0usize;
        while offset < total {
            context.ensure_active().map_err(map_context_error)?;
            let width = total.saturating_sub(offset).min(MAX_BATCH_ROWS);
            let window = origin.slice(offset, width);
            let mut working = crate::ffi::record_batch_to_dataframe(&window)?;
            run.memory.hold_polars(working.estimated_size())?;
            let mut working_schema = run.scan_output_logical.clone();
            let mut local_sidecar: Vec<(usize, u64)> = sidecar[offset..offset + width].to_vec();
            let mut findings_per_row = vec![0u32; width];

            for step in vsteps.iter().skip(scan_boundary) {
                match step {
                    VStep::Project { columns } => {
                        let compiled = [super::preflight::CompiledStep::Project {
                            columns: columns.clone(),
                        }];
                        let (next_frame, next_deferred) =
                            crate::lower::transform(working, &working_schema, &compiled, deferred)?;
                        deferred = next_deferred;
                        working = next_frame;
                        working_schema =
                            crate::preflight::project_schema(&working_schema, columns)?;
                    }
                    VStep::Filter { predicate } | VStep::FilterRows { predicate } => {
                        let keep = boolean_keep_mask(&working, &working_schema, predicate)?;
                        compact_alive(
                            &mut working,
                            &mut local_sidecar,
                            &mut findings_per_row,
                            &keep,
                        )?;
                    }
                    VStep::TransformRules { rules } => {
                        let compiled = [super::preflight::CompiledStep::Rules {
                            rules: rules.clone(),
                        }];
                        let (next_frame, next_deferred) =
                            crate::lower::transform(working, &working_schema, &compiled, deferred)?;
                        deferred = next_deferred;
                        working = next_frame;
                        for rule in rules {
                            working_schema =
                                crate::preflight::apply_rule_schema(working_schema, rule, true)?;
                        }
                    }
                    VStep::Validate {
                        node_id,
                        rule_ordinal,
                        predicate,
                        severity,
                        ..
                    } => {
                        let outcomes = evaluate_predicate(&working, &working_schema, predicate)?;
                        let summary_key = (node_id.as_u128(), *rule_ordinal);
                        let summary_index = run
                            .val_summary_index
                            .get(&summary_key)
                            .copied()
                            .ok_or(EngineError::Internal("validation summary missing"))?;
                        let severity_literal = severity_literal(*severity);
                        let outcome_literal = |outcome: Option<bool>| {
                            if outcome.unwrap_or(false) {
                                "false"
                            } else {
                                "null"
                            }
                        };
                        let mut dead = vec![false; outcomes.len()];
                        for (index, outcome_ref) in outcomes.iter().enumerate() {
                            let outcome = *outcome_ref;
                            match outcome {
                                Some(true) => {
                                    run.val_summaries[summary_index].tally.evaluated += 1;
                                    run.val_summaries[summary_index].tally.pass += 1;
                                }
                                Some(false) | None => {
                                    let outcome_text = outcome_literal(outcome);
                                    run.val_summaries[summary_index].tally.evaluated += 1;
                                    run.val_summaries[summary_index].tally.fail += 1;
                                    match severity {
                                        ValidationSeverity::Warning => {
                                            run.val_summaries[summary_index].tally.warning += 1;
                                        }
                                        ValidationSeverity::Error => {
                                            run.val_summaries[summary_index].tally.error += 1;
                                        }
                                    }
                                    match outcome {
                                        Some(true) => {}
                                        Some(false) => {
                                            run.val_summaries[summary_index]
                                                .tally
                                                .false_outcomes += 1;
                                        }
                                        None => {
                                            run.val_summaries[summary_index].tally.null_outcomes +=
                                                1;
                                        }
                                    }
                                    findings_per_row[index] += 1;
                                    if findings_per_row[index] as usize
                                        > MAX_VALIDATION_FINDINGS_PER_ROW
                                    {
                                        return Err(EngineError::BoundExceeded(
                                            "source row exceeded MAX_VALIDATION_FINDINGS_PER_ROW",
                                        ));
                                    }
                                    run.hold_validation_row()?;
                                    run.val_findings.push(ValFindingRow {
                                        ordinal: local_sidecar[index].1,
                                        node_id: *node_id,
                                        rule_ordinal: *rule_ordinal,
                                        severity: severity_literal,
                                        outcome: outcome_text,
                                    });
                                    if matches!(severity, ValidationSeverity::Error) {
                                        // First Error is terminal for this row:
                                        // rejected payload + no later rule sees it.
                                        dead[index] = true;
                                    }
                                }
                            }
                        }
                        if dead.iter().any(|dead| *dead) {
                            for (index, is_dead) in dead.iter().enumerate() {
                                if *is_dead {
                                    reject_row(
                                        run,
                                        &origin,
                                        offset + local_sidecar[index].0,
                                        local_sidecar[index].1,
                                        *node_id,
                                        *rule_ordinal,
                                        "validation_error",
                                        &inputs,
                                        writer,
                                        sinks,
                                        context,
                                    )?;
                                }
                            }
                            compact_alive(
                                &mut working,
                                &mut local_sidecar,
                                &mut findings_per_row,
                                &dead.iter().map(|dead| !dead).collect::<Vec<_>>(),
                            )?;
                        }
                    }
                    VStep::Deduplicate {
                        node_id,
                        rule_ordinal,
                        keys,
                        key_types,
                    } => {
                        let key = (node_id.as_u128(), *rule_ordinal);
                        let summary_index = run
                            .dedup_summary_index
                            .get(&key)
                            .copied()
                            .ok_or(EngineError::Internal("dedup summary missing"))?;
                        run.dedup_summaries[summary_index].evaluated += working.height() as u64;
                        let mut columns: Vec<polars::prelude::Series> =
                            Vec::with_capacity(keys.len());
                        for key_id in keys {
                            let name = working_schema
                                .field(*key_id)
                                .ok_or(EngineError::UnknownColumn(*key_id))?
                                .name
                                .clone();
                            let column = working
                                .column(&name)
                                .map_err(|_| EngineError::UnknownColumn(*key_id))?;
                            columns.push(column.as_materialized_series().clone());
                        }
                        let height = working.height();
                        let mut dead = vec![false; height];
                        for index in 0..height {
                            let mut encoded = KeyBytes::new();
                            for (series, declared) in columns.iter().zip(key_types.iter()) {
                                let value = key_component(series, index, declared)?;
                                encode_component(declared, value, &mut encoded)?;
                            }
                            let ordinal = local_sidecar[index].1;
                            match dedup
                                .insert_first(*node_id, *rule_ordinal, encoded.as_slice(), ordinal)
                                .map_err(map_verification_storage_error)?
                            {
                                DedupInsert::Inserted {
                                    first_source_row_ordinal: _,
                                } => {
                                    run.dedup_summaries[summary_index].unique += 1;
                                }
                                DedupInsert::Duplicate {
                                    first_source_row_ordinal,
                                } => {
                                    run.dedup_summaries[summary_index].duplicates += 1;
                                    run.hold_dedup_report_row()?;
                                    run.dup_findings.push(DupFindingRow {
                                        ordinal,
                                        first_ordinal: first_source_row_ordinal,
                                        node_id: *node_id,
                                        rule_ordinal: *rule_ordinal,
                                        key_column_count: keys.len() as u32,
                                        encoded_len: encoded.len() as u32,
                                    });
                                    dead[index] = true;
                                }
                            }
                        }
                        for (index, is_dead) in dead.iter().enumerate() {
                            if *is_dead {
                                reject_row(
                                    run,
                                    &origin,
                                    offset + local_sidecar[index].0,
                                    local_sidecar[index].1,
                                    *node_id,
                                    *rule_ordinal,
                                    "duplicate",
                                    &inputs,
                                    writer,
                                    sinks,
                                    context,
                                )?;
                            }
                        }
                        if dead.iter().any(|dead| *dead) {
                            compact_alive(
                                &mut working,
                                &mut local_sidecar,
                                &mut findings_per_row,
                                &dead.iter().map(|dead| !dead).collect::<Vec<_>>(),
                            )?;
                        }
                    }
                }
            }

            // Survivors become accepted rows (contract 5.3).
            if working.height() > 0 {
                let survivors = crate::ffi::dataframe_to_record_batch(
                    working,
                    &run.materialize_logical,
                    &run.materialize_arrow,
                    &deferred,
                )?;
                run.hold_accepted(survivors.get_array_memory_size())?;
                run.accepted_pending_rows += survivors.num_rows();
                run.accepted_batches.push(survivors);
                flush_accepted_packs(run, accepted_factory, writer, context)?;
            } else {
                run.memory.drop_polars()?;
            }
            offset += width;
        }
        run.memory.drop_envelope()?;
    }
    Ok(())
}

/// Terminal rejection of one source row (contract 5.3/8.4/10.5): the
/// original logical Scan-output values become the single payload row.
/// With `rejected_rows_artifact_id = None` the first terminal rejection
/// fails the run before any rejected writer append.
#[allow(clippy::too_many_arguments)]
fn reject_row(
    run: &mut VerificationRun,
    origin: &RecordBatch,
    origin_row: usize,
    ordinal: u64,
    node_id: Uuid,
    rule_ordinal: u32,
    kind: &'static str,
    inputs: &FlushInputs,
    writer: &mut VerificationBundleWriter,
    sinks: &mut ReportSinks,
    context: &RequestContext,
) -> Result<(), EngineError> {
    run.terminal_rejections += 1;
    if run.rejected_authorized.is_none() {
        return Err(EngineError::InvalidPlan(
            "the run declared no rejected artifact but a terminal rejection occurred",
        ));
    }
    let payload = origin.slice(origin_row, 1);
    let bytes = payload.get_array_memory_size();
    run.memory.hold_rejected_remainder(bytes)?;
    run.rejected.push(RejectedEntry {
        payload,
        kind,
        node_id,
        rule_ordinal,
        ordinal,
        bytes,
    });
    if run.rejected.len() >= stillflow_storage::artifact::REPORT_PACK_ROWS {
        flush_rejected_rows(run, &mut sinks.rejected, writer, inputs.clone(), context)?;
    }
    Ok(())
}

/// Evaluates one Validate predicate per row, preserving tri-state outcomes
/// (contract 5.2): `null` is always a failure, never an implicit pass.
fn evaluate_predicate(
    frame: &polars::prelude::DataFrame,
    schema: &stillflow_core::LogicalSchema,
    predicate: &Expr,
) -> Result<Vec<Option<bool>>, EngineError> {
    use polars::prelude::IntoLazy;
    let lowered = crate::lower::lower_expr(predicate, schema)?;
    let selected = frame
        .clone()
        .lazy()
        .select([lowered.alias("__sf_outcome")])
        .collect()
        .map_err(|_| EngineError::TypeError("predicate evaluation failed"))?;
    let column = selected
        .column("__sf_outcome")
        .map_err(|_| EngineError::Internal("predicate column missing"))?;
    let typed = column
        .as_materialized_series()
        .bool()
        .map_err(|_| EngineError::TypeError("predicate did not evaluate to Boolean"))?;
    Ok(typed.into_iter().collect())
}

impl ExecutionEngine {
    /// Deterministic E4 verification materialization (contract section 11).
    ///
    /// Publication sequence per contract 10.1; the commit is the sole
    /// visibility point and every failure path publishes nothing.
    pub async fn materialize_verification(
        &self,
        request: VerificationRequest<'_>,
    ) -> Result<VerificationBundle, EngineError> {
        let mut context = request.context.clone();
        if context.deadline().is_none() {
            context = RequestContext::with_cancellation_and_deadline(
                context.cancellation().clone(),
                tokio::time::Instant::now() + crate::ENGINE_DEFAULT_DEADLINE,
            );
        }
        // Checkpoint 1: before preflight inspection I/O.
        context.ensure_active().map_err(map_context_error)?;
        if request.batch_size < stillflow_core::ReadRequest::MIN_BATCH_SIZE
            || request.batch_size > stillflow_core::ReadRequest::MAX_BATCH_SIZE
        {
            return Err(EngineError::BoundExceeded(
                "batch_size is outside 1..=65536",
            ));
        }
        if context
            .remaining()
            .is_some_and(|remaining| remaining > crate::ENGINE_MAX_DEADLINE)
        {
            return Err(EngineError::BoundExceeded(
                "request deadline exceeds ENGINE_MAX_DEADLINE",
            ));
        }
        let permit = std::sync::Arc::clone(self.run_gate())
            .try_acquire_owned()
            .map_err(|_| EngineError::Busy)?;
        let result = self
            .materialize_verification_inner(request, context, &permit)
            .await;
        drop(permit);
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn materialize_verification_inner(
        &self,
        request: VerificationRequest<'_>,
        context: RequestContext,
        _permit: &tokio::sync::OwnedSemaphorePermit,
    ) -> Result<VerificationBundle, EngineError> {
        // Shared E2 preflight with the E4 target enabled plus E4 checks
        // (checkpoint covered by ensure_active inside). The verification
        // path has no preview target.
        let prepared = super::preflight::preflight_inner(
            self.registry(),
            &request.plan,
            &request.connection,
            &request.asset,
            request.schema_override.as_ref(),
            &context,
            None,
            true,
        )
        .await?;
        let vplan = build_verification_plan(&request.plan, &prepared)?;

        // The engine recomputes the canonical-plan SHA-256 and rejects a
        // caller mismatch (contract 10.5 / V24).
        let canonical_bytes = request
            .plan
            .canonical_bytes()
            .map_err(|_| EngineError::InvalidPlan("logical plan canonicalization failed"))?;
        use sha2::Digest as _;
        let canonical_plan_digest: [u8; 32] = sha2::Sha256::digest(&canonical_bytes).into();
        if canonical_plan_digest != request.identities.canonical_plan_digest {
            return Err(EngineError::InvalidPlan("canonical plan digest mismatch"));
        }
        let plan_fingerprint = *request
            .plan
            .fingerprint()
            .as_ref()
            .map_err(|_| EngineError::Internal("logical plan fingerprint failed"))?
            .as_bytes();

        validate_verification_identities(&request.identities, request.asset.id)?;

        let expected_fingerprint =
            stillflow_core::LogicalSchemaFingerprint::try_from_schema(&prepared.expected_connector)
                .map_err(|_| EngineError::Internal("connector schema fingerprint failed"))?;

        // Bundle provenance draft carries the recomputed digest (contract 7.2).
        let bundle_provenance = bundle_provenance_draft(
            &request.identities,
            request.asset.id,
            plan_fingerprint,
            canonical_plan_digest,
            crate::ENGINE_BUILD,
        );
        let accepted_draft = SnapshotDraft::try_new(
            request.identities.snapshot_id,
            request.identities.dataset_id,
            request.identities.session_id,
            request.asset.id,
            prepared.materialize_schema.clone(),
            request.identities.lineage.clone(),
            request.identities.quality_score,
            request.identities.created_at,
        )
        .map_err(map_verification_storage_error)?;
        let draft = VerificationBundleDraft::try_new(
            bundle_provenance,
            accepted_draft,
            request.identities.validation_report_artifact_id,
            request.identities.rejected_rows_artifact_id,
            request.identities.deduplication_report_artifact_id,
        )
        .map_err(map_verification_storage_error)?
        // Issue #176 (D2), wired by E4-S2-REBIND-R2: the rejected artifact is
        // bound to the frozen logical Scan-output schema — NOT to the
        // materialized/post-rule schema — so terminal rejections keep their
        // original row schema and values across Drop/Rename/Cast/Derive.
        .with_rejected_source_schema(prepared.scan_output.clone());

        // Step 4: exactly one storage publisher permit + staging context.
        let mut writer = request
            .store
            .begin_verification_bundle(draft, request.identities.started_at)
            .map_err(map_verification_storage_error)?;
        // Step 5: the temporary dedup index.
        let dedup_index = request
            .store
            .open_dedup_index(
                request.identities.run_id,
                request.identities.bundle_id,
                request.identities.started_at,
            )
            .map_err(map_verification_storage_error);

        let index = match dedup_index {
            Ok(index) => index,
            Err(error) => {
                drop(writer);
                return Err(error);
            }
        };

        // Per-rule summary slots in preflight execution order.
        let mut val_summaries = Vec::new();
        let mut val_summary_index = std::collections::BTreeMap::new();
        let mut dedup_summaries = Vec::new();
        let mut dedup_summary_index = std::collections::BTreeMap::new();
        for step in vplan.vsteps.iter() {
            match step {
                VStep::Validate {
                    node_id,
                    rule_ordinal,
                    message,
                    ..
                } => {
                    val_summary_index
                        .insert((node_id.as_u128(), *rule_ordinal), val_summaries.len());
                    val_summaries.push(ValSummaryRow {
                        node_id: *node_id,
                        rule_ordinal: *rule_ordinal,
                        message: message.clone(),
                        tally: ValidationTally::new(),
                    });
                }
                VStep::Deduplicate {
                    node_id,
                    rule_ordinal,
                    keys,
                    ..
                } => {
                    dedup_summary_index
                        .insert((node_id.as_u128(), *rule_ordinal), dedup_summaries.len());
                    dedup_summaries.push(DedupSummaryRow {
                        node_id: *node_id,
                        rule_ordinal: *rule_ordinal,
                        key_column_count: keys.len() as u32,
                        evaluated: 0,
                        unique: 0,
                        duplicates: 0,
                    });
                }
                _ => {}
            }
        }

        let compiled_budget = crate::preflight::compiled_plan_bytes(&request.plan);
        let rejected_arrow_schema = stillflow_core::logical_schema_to_arrow(
            &stillflow_storage::artifact::rejected_rows_section_schema(&prepared.scan_output)
                .map_err(map_verification_storage_error)?,
        )
        .map_err(|_| EngineError::Internal("rejected arrow schema failed"))?;

        let mut run = VerificationRun {
            vplan,
            flush: FlushInputs::new(
                request.asset.id,
                &request.identities.logical_input,
                &plan_fingerprint,
                &canonical_plan_digest,
            ),
            scan_output_logical: prepared.scan_output.clone(),
            scan_output_arrow: stillflow_core::logical_schema_to_arrow(&prepared.scan_output)
                .map_err(|_| EngineError::Internal("scan output arrow schema failed"))?,
            scan_output_fields: prepared.scan_output.fields.len(),
            scan_output_rejected_arrow: rejected_arrow_schema,
            materialize_logical: prepared.materialize_schema.clone(),
            materialize_arrow: stillflow_core::logical_schema_to_arrow(
                &prepared.materialize_schema,
            )
            .map_err(|_| EngineError::Internal("materialize arrow schema failed"))?,
            batch_size: request.batch_size,
            next_ordinal: 0,
            memory: VerificationMemory::default(),
            val_summaries,
            val_summary_index,
            val_findings: Vec::new(),
            dedup_summaries,
            dedup_summary_index,
            dup_findings: Vec::new(),
            rejected: Vec::new(),
            accepted_batches: Vec::new(),
            accepted_pending_rows: 0,
            accepted_buffered_bytes: 0,
            accepted_emitted_sequences: 0,
            terminal_rejections: 0,
            rejected_authorized: request.identities.rejected_rows_artifact_id,
        };
        run.memory.check_operator_state(compiled_budget)?;

        let mut sinks = ReportSinks {
            val_summary: PackSink::new(
                stillflow_storage::artifact::validation_rule_summary_section_schema(),
                request.asset.id,
            )?,
            val_finding: PackSink::new(
                stillflow_storage::artifact::validation_finding_section_schema(),
                request.asset.id,
            )?,
            rejected: PackSink::new(
                stillflow_storage::artifact::rejected_rows_section_schema(&prepared.scan_output)
                    .map_err(map_verification_storage_error)?,
                request.asset.id,
            )?,
            dedup_summary: PackSink::new(
                stillflow_storage::artifact::dedup_rule_summary_section_schema(),
                request.asset.id,
            )?,
            dup_finding: PackSink::new(
                stillflow_storage::artifact::duplicate_finding_section_schema(),
                request.asset.id,
            )?,
        };
        let accepted_factory = BatchEnvelopeFactory::try_new(
            StdArc::new(prepared.materialize_schema.clone()),
            request.asset.id,
        )
        .map_err(|_| EngineError::Internal("accepted envelope factory build failed"))?;

        let mut dedup = Some(index);
        let streamed = self
            .stream_verification(
                &request,
                &context,
                &prepared,
                &expected_fingerprint,
                &mut run,
                dedup.as_ref().expect("dedup index present"),
                &mut writer,
                &mut sinks,
                &accepted_factory,
            )
            .await;

        match streamed {
            Ok(()) => {}
            Err(error) => {
                // Normal cleanup: dropping the writer aborts the whole
                // staging context; the dedup index Drop is best-effort and
                // any residue is recoverable under the maintenance gate
                // (contract 10.3). No bundle is published.
                drop(writer);
                drop(dedup);
                return Err(error);
            }
        }

        // Accepted tail remainder (checkpoint 5 emission point).
        finish_accepted(&mut run, &accepted_factory, &mut writer, &context)?;

        // Checkpoint 7: before close_and_delete.
        context.ensure_active().map_err(map_context_error)?;
        let closed = dedup
            .take()
            .expect("dedup index present")
            .close_and_delete()
            .map_err(map_verification_storage_error);
        if let Err(error) = closed {
            drop(writer);
            return Err(error);
        }

        // Final report emission before commit (checkpoint 6).
        let flush_inputs = run.flush.clone();
        if let Err(error) = flush_validation_rule_summaries(
            &mut run,
            &mut sinks.val_summary,
            &mut writer,
            flush_inputs.clone(),
            &context,
        ) {
            drop(writer);
            return Err(error);
        }
        if let Err(error) = flush_dedup_rule_summaries(
            &mut run,
            &mut sinks.dedup_summary,
            &mut writer,
            flush_inputs.clone(),
            &context,
        ) {
            drop(writer);
            return Err(error);
        }
        if let Err(error) = flush_validation_findings(
            &mut run,
            &mut sinks.val_finding,
            &mut writer,
            flush_inputs.clone(),
            &context,
        ) {
            drop(writer);
            return Err(error);
        }
        if let Err(error) = flush_duplicate_findings(
            &mut run,
            &mut sinks.dup_finding,
            &mut writer,
            flush_inputs.clone(),
            &context,
        ) {
            drop(writer);
            return Err(error);
        }
        if let Err(error) = flush_rejected_rows(
            &mut run,
            &mut sinks.rejected,
            &mut writer,
            flush_inputs.clone(),
            &context,
        ) {
            drop(writer);
            return Err(error);
        }

        // Checkpoint 8: the single visibility point.
        context.ensure_active().map_err(map_context_error)?;
        let committed = writer.commit(request.identities.committed_at);
        match committed {
            Ok(bundle) => Ok(bundle),
            Err(error) => Err(map_verification_storage_error(error)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn stream_verification(
        &self,
        request: &VerificationRequest<'_>,
        context: &RequestContext,
        prepared: &super::preflight::PreparedPlan,
        expected_fingerprint: &stillflow_core::LogicalSchemaFingerprint,
        run: &mut VerificationRun,
        dedup: &DedupIndex,
        writer: &mut VerificationBundleWriter,
        sinks: &mut ReportSinks,
        accepted_factory: &BatchEnvelopeFactory,
    ) -> Result<(), EngineError> {
        // Checkpoint 2: before opening read_batches.
        context.ensure_active().map_err(map_context_error)?;
        let read = stillflow_core::ReadRequest {
            context: context.clone(),
            asset: request.asset.clone(),
            schema_override: Some(prepared.expected_connector.clone()),
            projection: prepared
                .push_projection
                .then(|| prepared.scan_projection.clone()),
            filter: None,
            checkpoint: None,
            batch_size: request.batch_size,
        };
        let mut stream = self
            .registry()
            .read_batches(&request.connection, read)
            .await
            .map_err(EngineError::from_connector)?;

        while let Some(item) = stream.next().await {
            // Checkpoint 3: on every connector stream poll.
            context.ensure_active().map_err(map_context_error)?;
            let envelope = item.map_err(EngineError::from_connector)?;
            process_envelope(
                run,
                prepared,
                expected_fingerprint,
                envelope,
                dedup,
                writer,
                sinks,
                accepted_factory,
                context,
            )?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// V14 evidence: the six-slot memory law exercised directly (contract 12.1/12.2)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod verification_memory_law_tests {
    use super::*;

    /// Holding all six slot kinds makes exactly six payloads live; releasing
    /// one drops the count; a slot never double-counts.
    #[test]
    fn six_slot_ceiling_is_exact_and_slots_die_when_emptied() {
        let mut memory = VerificationMemory::default();
        memory.hold_envelope(8).expect("envelope");
        memory.hold_polars(8).expect("polars");
        memory.hold_accepted_remainder(8).expect("accepted");
        memory.hold_rejected_remainder(8).expect("rejected");
        memory.hold_validation_report(8).expect("validation");
        memory.hold_dedup_report(8).expect("dedup");
        assert_eq!(memory.live_payloads, 6);
        memory.drop_envelope().expect("drop envelope");
        assert_eq!(memory.live_payloads, 5);
        assert!(!memory.slots[VerificationMemory::SLOT_ENVELOPE]);
        memory.hold_envelope(8).expect("re-arm envelope slot");
        assert_eq!(memory.live_payloads, 6);
    }

    /// A bounded slot refuses bytes beyond its cap even while live.
    #[test]
    fn slot_cap_refuses_bytes_beyond_the_batch_budget() {
        let mut memory = VerificationMemory::default();
        memory
            .hold_envelope(stillflow_core::MAX_BATCH_BYTES)
            .expect("full envelope slot");
        let error = memory.hold_envelope(1).expect_err("slot cap must refuse");
        assert!(matches!(error, EngineError::BoundExceeded(_)));
        memory.drop_envelope().expect("release");
        assert_eq!(memory.live_payloads, 0);
    }

    /// The engine-peak law: four full batch slots plus two full report slots
    /// fit exactly; one more byte anywhere exceeds the 265 MiB peak.
    #[test]
    fn engine_peak_law_admits_the_exact_six_slot_sum_only() {
        let mut memory = VerificationMemory::default();
        memory
            .hold_envelope(stillflow_core::MAX_BATCH_BYTES)
            .expect("envelope");
        memory
            .hold_polars(stillflow_core::MAX_BATCH_BYTES)
            .expect("polars");
        memory
            .hold_accepted_remainder(stillflow_core::MAX_BATCH_BYTES)
            .expect("accepted");
        memory
            .hold_rejected_remainder(stillflow_core::MAX_BATCH_BYTES)
            .expect("rejected");
        memory
            .hold_validation_report(stillflow_storage::artifact::REPORT_PACK_BYTES)
            .expect("validation");
        memory
            .hold_dedup_report(stillflow_storage::artifact::REPORT_PACK_BYTES)
            .expect("dedup");
        assert_eq!(memory.live_payloads, 6);
        let error = memory
            .hold_polars(1)
            .expect_err("polars slot is empty but the peak is full");
        assert!(matches!(error, EngineError::BoundExceeded(_)));
    }

    /// The operator-state law: compiled plan + FFI scratch + routing +
    /// configured dedup cache must stay within the 5 MiB engine budget.
    #[test]
    fn operator_state_law_bounds_the_engine_side_sum() {
        let mut memory = VerificationMemory::default();
        memory
            .hold_routing(VERIFICATION_MAX_ROUTING_STATE_BYTES)
            .expect("full routing budget");
        memory
            .check_operator_state(VERIFICATION_MAX_COMPILED_PLAN_BYTES)
            .expect("exact operator-state sum fits");
        let error = memory
            .check_operator_state(VERIFICATION_MAX_COMPILED_PLAN_BYTES + 1)
            .expect_err("one byte over the operator-state budget");
        assert!(matches!(error, EngineError::BoundExceeded(_)));
        let error = memory
            .hold_routing(1)
            .expect_err("routing budget is exhausted");
        assert!(matches!(error, EngineError::BoundExceeded(_)));
    }
}
