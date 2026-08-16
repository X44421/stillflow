# Issue #54 Implementation Contract: validation, rejected rows, and exact deduplication (E4-C0-R1)

> Status: Frozen for architecture review (not approved)
> Revision: C0-R1
> Supersedes: C0 `d33f45610620c03afe253cdc0b4aef7468fa5dd8`
> Risk: High
> Issue: #54 (contract)
> PR: #57 draft (actual PR number; C0 header said expected #55 and is corrected here)
> Parent: Issue #46 revision R3, merged at
> `32f1c53d9903f66aeaca1c2676c0b81abfb2a702` in PR #47
> Authorized base: `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`
> Branch: `agent/issue-054-validation-rejected-rows-contract`
> Last updated: 2026-08-16
> Review: PR #57 remains draft with Request changes. C0 was not approved.
> R1 closes the six P0 blockers and the validation-message safety fact
> error. Architecture approval binds exactly one new commit SHA of this
> file. E4 runtime remains paused until that approval and must then rebuild
> from the latest accepted `main`.

This document freezes the public contract and objective acceptance matrix for
Engine E4-C0: `Rule::Validate`, rejected rows, and exact `Rule::Deduplicate`.
It does **not** authorize any Rust runtime code, dependency change, lockfile
change, CI change, or frontend change.

## 1. Objective

Freeze a single deterministic verification-publication path that:

1. assigns a connector-strategy-stable, zero-based `source_row_ordinal` at the
   logical Scan output boundary;
2. executes `Rule::Validate` with frozen true / false / null semantics and
   emits row-level `ValidationFinding` values, not lifecycle events;
3. executes `Rule::Deduplicate` with exact, stable keep-first semantics over
   full canonical key bytes in a recoverable SQLite index;
4. publishes one atomic `VerificationBundle`: accepted snapshot, validation
   report, optional rejected rows artifact, deduplication report, and one
   unified provenance;
5. keeps every duplicate/rejected/report buffer bounded and external where
   required;
6. reuses the E2 preflight, typing, lowering, chunker, Arrow interchange,
   error taxonomy, and sanitized-error semantics from Issue #46.

E4-C0 must not create a second cleaning language, a second executor, or a
separate Preview interpretation of Validate/Deduplicate.

## 2. Source policy and branch discipline

- The authorized base is `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`.
- This branch is created from that exact commit. It must not merge, rebase
  onto, or cherry-pick from PR #53, PR #49, or any historical branch.
- PR #53 is a read-only reference for the current E3 public surface. E4
  runtime must wait until PR #53 merges and must then rebuild from the
  latest accepted `main`; this contract does not authorize changing E3 or
  workflow/architecture files now.
- This delivery is docs-only. The only authorized file is
  `docs/issues/issue-054-validation-rejected-rows-contract.md`.
- No Rust source, `Cargo.toml`, `Cargo.lock`, frontend file, CI workflow,
  architecture file, or Issue #46 / #48 / #50 contract may be modified.
- Every public type or field outside the current PR #53 public API is named
  `Proposed` in section 11 and is not implemented in this PR.

## 3. Risk, compatibility, and C0 review disposition

This work is `risk:high` because it defines a multi-artifact atomic
verification publication, rejected-row data plane, artifact provenance,
exact key equality, and external deduplication state used by all later
E5 job/API work.

### 3.1 C0 blockers closed by R1

| C0 blocker | R1 disposition |
| --- | --- |
| Warning / validation error / duplicate mixed in one rejected snapshot with up to 256 full-row copies | Separated `ValidationReportArtifact`, optional `RejectedRowsArtifact`, and `DeduplicationReportArtifact` (sections 5 and 8). Warning findings never enter rejected rows. A terminal rejection stores at most one original-row payload per source row. |
| `begin_snapshot_pair` permanently fixes storage to two snapshots | Replaced by one atomic `VerificationBundle` (section 10). Zero rejections produce no empty `DatasetSnapshot` for rejected rows. |
| No unified artifact provenance | Frozen `ArtifactProvenance`, `InputRef`, `SourceRowRef`, `RuleRef` and required fields in section 7. |
| `source_row_ordinal` defined before `Scan.predicate` and therefore depends on physical predicate-pushdown strategy | Ordinal is assigned at the logical Scan output boundary after projection and `Scan.predicate` semantics, regardless of which layer executes the predicate (section 5). |
| Canonical key not injective and crosses the E2 type boundary | Timestamp timezone now has a presence tag. E4-C0 pauses `List`, `Struct`, and `Timestamp { unit: Second }`, matching the E2 typing boundary (section 6). |
| Dedup index deletes an existing same-id file and could destroy an active request | `create_new` exclusive open, ownership lease, no blind delete, explicit `close_and_delete() -> Result`, recovery cleanup, `PRAGMA max_page_count`, `0700` directory / `0600` file (section 9). |
| Fact error: claim that `Rule::validate` already performs `ensure_no_secret_fields` for the validation message | R1 requires an explicit E4 preflight security check and does not rely on the indirect `Rule::validate` path (section 10.6). |

### 3.2 Compatibility decision

- Existing `ExecutionRequest`, `ExecutionIdentities`, `ExecutionEngine`,
  `PreviewRequest`, `PreviewResult`, `BatchEnvelope`, `LogicalSchema`,
  `ColumnId`, `Rule`, `Expr`, and `SnapshotDraft` version 1 contracts are
  not changed by this document.
- `ExecutionEngine::materialize` keeps the frozen E2 behavior and keeps
  returning `UnsupportedRule` for `Rule::Validate` / `Rule::Deduplicate`.
- E4 adds a new `materialize_verification` entry point and new verification
  types in section 11. They are `Proposed` until the later runtime PR and
  must be reconciled against the then-current E3 API after PR #53 merges.
- Existing storage single-snapshot `begin_snapshot` / `commit` remain valid.
  E4 proposes an additive `VerificationBundle` API; it does not redefine
  the old API and does not force the old storage protocol to know about
  verification artifacts.
- No compatibility shim is provided for Join/Union execution, DuckDB SQL,
  SQLx, arbitrary engine code, approximate deduplication, or hash-only
  deduplication.

## 4. Scope

In scope:

- Logical Scan output identity and row routing.
- Validate semantics and `ValidationFinding` model.
- Exact Deduplicate key equality, canonical key bytes, and SQLite index.
- `VerificationBundle`, its four possible artifacts, and provenance.
- Atomic publication, identity/time injection, cancellation, and cleanup.
- Preview relationship.
- Security boundary for raw values and validation messages.
- Numeric resource ceilings and the V01–V24 acceptance matrix.

Explicit non-goals:

- HTTP routes, Axum handlers, job tables, or E5 status machines.
- Frontend layout, components, CSS, tokens, or generated types.
- DuckDB, SQLx, ConnectorX, or SQL Connector #9.
- `Join` / `Union` execution.
- A second Validate/Deduplicate Preview implementation.
- AI execution, Python, SQL strings, or arbitrary Polars/Python/SQL programs.
- Sampling, reservoir, random selection, or approximate duplicate detection.
- Dependabot updates mixed into this branch.

## 5. Row identity and routing

### 5.1 Logical Scan output identity

`source_row_ordinal` is a `u64`, zero-based, assigned exactly once per row
of the **logical Scan output stream**:

```text
connector rows
  -> Scan projection (whether executed by the connector or by the engine)
  -> Scan.predicate (whether executed by the connector or by the engine)
```

Assignment rules:

1. Rows are assigned ordinals **after** both projection and
   `Scan.predicate` semantics, regardless of whether the predicate is
   physically pushed down or executed in-engine. A future pushdown change
   therefore cannot renumber or expose different rows.
2. Ordinal `0` is the first surviving logical Scan output row. Later
   surviving rows receive `previous + 1` with checked addition.
3. Rows dropped by `Scan.predicate` are never assigned ordinals and are
   not referenceable by any E4 artifact. This is the R1 correction to C0.
4. Later `Filter` / `FilterRows` nodes drop rows without renumbering.
   Ordinals of later surviving rows may contain gaps. Gaps are stable and
   intentional; consumers must not infer row counts from the maximum
   ordinal.
5. Ordinals follow logical row order, never connector envelope sequence,
   physical partitions, file offsets, or the layer that executed
   `Scan.predicate`.
6. At most `MAX_SNAPSHOT_ROWS` logical Scan output rows may enter the
   cleaning path. Checked overflow or exceeding `MAX_SNAPSHOT_ROWS` is
   `EngineError::BoundExceeded`.
7. The rejected rows artifact copies the logical Scan output row values for
   its terminal row payload. Predicate filtering does not alter values, so
   this payload is independent of predicate execution strategy.

### 5.2 Validate true / false / null semantics

For `Rule::Validate { predicate, severity, message }`, preflight type-checks
the predicate against the current working schema exactly like any E2
Boolean expression. The predicate must infer to `LogicalType::Boolean`;
otherwise preflight returns `EngineError::TypeError`.

At runtime, for each input row:

| Predicate result | `severity = Warning` | `severity = Error` |
| --- | --- | --- |
| Boolean `true` | pass; no finding | pass; no finding |
| Boolean `false` | failure; keep row in accepted stream; emit one warning `ValidationFinding` | failure; remove row from accepted stream; emit one error `ValidationFinding` and at most one rejected-row payload; stop evaluating that row |
| `null` | same as `false` | same as `false` |

`null` is always a validation failure, never an implicit pass.

Multi-rule collection rule:

- Rules are evaluated in listed order inside each `ApplyRules` node and in
  plan order across nodes.
- Warning findings are collected and the row continues to later rules.
- The first Error failure is terminal for that source row: the row is
  removed from the accepted stream and no later rule, operator, or node
  sees it. Warning findings already emitted are retained.
- A row removed by Error is never re-admitted and never promoted by a later
  deduplication rule.
- Hard cap: at most `MAX_VALIDATION_FINDINGS_PER_ROW` findings may be
  emitted for one source row across the whole run. Exceeding the cap is
  `EngineError::BoundExceeded`; no bundle is published.
- The term `Event` is reserved for future E5 lifecycle events. E4 artifacts
  contain `ValidationFinding` and `DuplicateFinding` rows only.

### 5.3 Routing summary

| Outcome | Accepted stream | Validation report | Rejected rows artifact | Dedup report |
| --- | --- | --- | --- | --- |
| Validate predicate `true` | row continues | none | none | none |
| Validate false/null, Warning | row continues | one warning finding | none | none |
| Validate false/null, Error | row removed | one error finding | one payload for that source row | none |
| Deduplicate first occurrence | row continues | none | none | none |
| Deduplicate later occurrence | row removed | none | one payload for that source row | one duplicate finding |

Rows dropped by `Scan.predicate` (at logical Scan output), `Filter`, or
`FilterRows` are silently dropped exactly as in E2. They produce no
finding, no rejected payload, and no duplicate finding.

## 6. Deduplicate exact semantics

### 6.1 Rule contract

`Rule::Deduplicate { keys }`:

- `keys` is the ordered `Vec<ColumnId>` already validated by
  `stillflow-plan` (non-empty, no duplicate ids).
- E4 preflight adds: `keys.len() <= MAX_DEDUP_KEY_COLUMNS`; every key id
  exists in the current working schema at that rule; every key working type
  passes the E2 `reject_paused_type` boundary and is in the E4-C0 supported
  set of section 6.3; and the canonical encoded composite key length can
  never exceed `MAX_DEDUP_KEY_BYTES`.
- Violations map to `UnknownColumn`, `TypeError`, or `BoundExceeded` as in
  section 10.7.
- Deduplicate does not change the working schema, row values, or row order.
- Multiple `Deduplicate` rules are independent namespaces keyed by
  `(node_id, rule_ordinal)`. A row may be first in one rule and duplicate
  in another; the first duplicate rule that fires is terminal for that row.
- Keep-first is decided solely by ascending `source_row_ordinal`. There is
  no tie: one logical Scan output row has exactly one ordinal. If the first
  row of a key class is later rejected by a subsequent Validate Error, the
  class has no accepted row; later duplicates are **not** promoted.

### 6.2 Key equality

Equality is exact, typed tuple equality over the ordered key columns. No
hash digest, no Unicode normalization, no collation, no trimming, no
approximate or phonetic comparison is permitted.

| Logical value | Frozen equality rule |
| --- | --- |
| `Null` | All nulls are equal to each other. Null is never equal to a non-null value. |
| `Boolean` | `false` and `true` exact. |
| `Int8/16/32/64`, `UInt8/16/32/64` | Exact numeric value. Typed components: values of different component types are different even when numerically equal. |
| `Float32` / `Float64` | All NaN values are equal to each other regardless of sign or payload. `-0.0` equals `+0.0`. Finite values compare by exact IEEE value. No epsilon. |
| `Utf8` | Exact UTF-8 byte sequence. Empty string is distinct from null. No normalization, case folding, or collation. |
| `Binary` | Exact byte sequence. Empty binary is distinct from null. |
| `Date32` | Exact days-since-epoch value; null equal to null. |
| `Timestamp { unit: Millisecond }` | Equal only when the component type is identical and the integer epoch count in that unit is identical. Null equal to null. |
| `Timestamp { unit: Microsecond }` | Same rule as Millisecond. |
| `Timestamp { unit: Nanosecond }` | Same rule as Millisecond. |

Within one `Deduplicate` rule each key id has one fixed working type, so
cross-type key coercion never occurs. The typed-tuple rule remains explicit
so a future engine cannot silently coerce key components.

### 6.3 E4-C0 supported key type boundary

The E2 typing boundary already pauses `LogicalType::List`,
`LogicalType::Struct`, and
`LogicalType::Timestamp { unit: TimeUnit::Second }`. E4-C0 preserves that
boundary:

- Supported key types: `Null`, `Boolean`, `Int8`, `Int16`, `Int32`, `Int64`,
  `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, `Float64`, `Utf8`,
  `Binary`, `Date32`, and `Timestamp` with unit `Millisecond`,
  `Microsecond`, or `Nanosecond`.
- Paused in E4-C0: `List`, `Struct`, and `Timestamp { unit: Second }`.
  A dedup key of any paused type is preflight `EngineError::TypeError`.
- Canonical tags `0x10` (List) and `0x11` (Struct) are reserved and are
  not emitted by E4-C0. Timestamp unit tag `0` (Second) is reserved and is
  never emitted by E4-C0.

### 6.4 Canonical key bytes

The SQLite index stores the full canonical key bytes. The encoding is
injective for every supported type and is not a hash.

`encode_component(declared_type, value)`:

1. If `value` is null: emit the single byte `0x00`.
2. Otherwise emit one type-tag byte followed by the type payload:

| Tag | Type | Payload |
| --- | --- | --- |
| `0x01` | `Boolean` | one byte, `0x00` or `0x01` |
| `0x02` | `Int8` | one byte, little-endian two's complement |
| `0x03` | `Int16` | two bytes, little-endian two's complement |
| `0x04` | `Int32` | four bytes, little-endian two's complement |
| `0x05` | `Int64` | eight bytes, little-endian two's complement |
| `0x06` | `UInt8` | one byte, little-endian |
| `0x07` | `UInt16` | two bytes, little-endian |
| `0x08` | `UInt32` | four bytes, little-endian |
| `0x09` | `UInt64` | eight bytes, little-endian |
| `0x0A` | `Float32` | four canonical IEEE bits, little-endian |
| `0x0B` | `Float64` | eight canonical IEEE bits, little-endian |
| `0x0C` | `Utf8` | `u32` little-endian byte length, then exact UTF-8 bytes |
| `0x0D` | `Binary` | `u32` little-endian byte length, then exact bytes |
| `0x0E` | `Date32` | four bytes, little-endian `i32` days since epoch |
| `0x0F` | `Timestamp { unit, timezone }` | one unit-tag byte (`1=Millisecond`, `2=Microsecond`, `3=Nanosecond`), one timezone-presence byte (`0=None`, `1=Some`), then for `Some`: `u32` little-endian UTF-8 byte length followed by the timezone bytes; for `None`: no length and no bytes. Finally eight bytes little-endian `i64` epoch count. |
| `0x10` | `List(element)` | reserved; E4-C0 preflight `TypeError` |
| `0x11` | `Struct(fields)` | reserved; E4-C0 preflight `TypeError` |

Float canonicalization before encoding:

- any NaN becomes the single canonical quiet-NaN bits
  `0x7FC00000` (Float32) or `0x7FF8000000000000` (Float64);
- any zero becomes positive zero bits;
- finite non-zero values keep their exact IEEE bits.

The timestamp timezone-presence byte is mandatory and makes
`Timestamp { unit, timezone: None }` and any `Some` encoding unambiguously
different. `Some("")` is already invalid under `LogicalType::validate`, but
even a hypothetical empty `Some` value would be length-prefixed after the
presence byte and therefore distinct from `None`.

`canonical_key_bytes` is the concatenation, in `keys` order, of
`encode_component(current_working_type(key_id), key_value)`. If the encoded
length exceeds `MAX_DEDUP_KEY_BYTES`, the run fails with
`EngineError::BoundExceeded` before the SQLite insert.

Injectivity law: for any two rows and the same key schema, different
supported key values produce different `canonical_key_bytes`; equal key
values under section 6.2 produce identical bytes. Tests use golden vectors
for every supported tag, including the timestamp presence cases.

## 7. Unified artifact provenance

### 7.1 Required provenance types

All E4 artifacts share one provenance model. These are `Proposed` domain
types and are serialized in storage manifests, not in `EngineError`.

```rust
pub enum InputRef {
    Asset { asset_id: Uuid },
    Snapshot { snapshot_id: Uuid },
}

pub struct SourceRowRef {
    pub input_ref: InputRef,
    pub source_row_ordinal: u64,
}

pub struct RuleRef {
    pub plan_fingerprint: PlanFingerprint,
    pub node_id: PlanNodeId,
    pub rule_ordinal: u32,
}

pub enum ArtifactKind {
    VerificationBundle,
    AcceptedSnapshot,
    ValidationReport,
    RejectedRows,
    DeduplicationReport,
}

pub struct ArtifactSummary {
    pub row_count: u64,
    pub stored_byte_count: u64,
    pub partition_count: u32,
    pub finding_count: u64,
    pub warning_count: u64,
    pub error_count: u64,
    pub duplicate_count: u64,
}

pub struct ArtifactProvenance {
    pub run_id: Uuid,
    pub artifact_id: Uuid,
    pub artifact_kind: ArtifactKind,
    pub input_ref: InputRef,
    pub plan_fingerprint: PlanFingerprint,
    pub engine_contract_version: u16,
    pub verification_contract_version: u16,
    pub lineage: BTreeSet<Uuid>,
    pub created_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub committed_at: DateTime<Utc>,
    pub summary: ArtifactSummary,
}
```

### 7.2 Required provenance fields

| Field | Rule |
| --- | --- |
| `run_id` | Caller-injected non-nil `Uuid`. One run has exactly one `VerificationBundle`. |
| `artifact_id` | Caller-injected non-nil `Uuid`. For the bundle provenance it equals `run_id`. Every child artifact id must be distinct from `run_id` and from every sibling artifact id. |
| `artifact_kind` | One of the five kinds. The bundle-level provenance uses `VerificationBundle`; child artifacts use their own kind. |
| `input_ref` | Caller-injected. E4-C0 uses only `InputRef::Asset { asset_id }` and requires it to equal the bound `SourceAsset.id`. `Snapshot` is reserved for future snapshot-input E4 revisions. |
| `plan_fingerprint` | `LogicalPlan::fingerprint()` computed by `stillflow-plan`; no second algorithm. |
| `engine_contract_version` | Existing `ENGINE_CONTRACT_VERSION`. |
| `verification_contract_version` | New `VERIFICATION_CONTRACT_VERSION = 1`. |
| `lineage` | Caller-injected `BTreeSet<Uuid>`; nil ids rejected. |
| `created_at` / `started_at` / `committed_at` | All caller-injected; engine must not call `Utc::now`. Ordering checked as `created_at <= started_at <= committed_at`. |
| `summary` | Sanitized counts and byte/partition totals only. It must never contain cell values, key bytes, validation message text, or unsanitized paths. |

The bundle-level provenance uses `ArtifactKind::VerificationBundle` and
`artifact_id = run_id`. Every child artifact uses its own artifact kind and
caller-injected artifact id. `VerificationBundleDraft` in section 11 carries
the same fields plus the accepted `SnapshotDraft` and the report/rejected
artifact ids; it is validated before any storage I/O.

`RuleRef` is stored in validation and deduplication report rows and in the
rejected rows control columns. It always contains `plan_fingerprint`,
`node_id`, and `rule_ordinal`, so a report row can be traced back to one
exact rule in one exact plan without joining plan-local ordinal counters.

`SourceRowRef` is flattened into artifact rows as `input_kind`, `input_id`,
and `source_row_ordinal` columns. E4-C0 writes `input_kind = "asset"` and
`input_id = SourceAsset.id`.

## 8. VerificationBundle artifact model

### 8.1 Artifacts

`materialize_verification` publishes exactly one `VerificationBundle`:

| Artifact | Presence | Row model | Content |
| --- | --- | --- | --- |
| Accepted snapshot | always | existing E2 `SnapshotManifest` rows | Final Materialize working schema and accepted rows in `source_row_ordinal` order. |
| `ValidationReportArtifact` | always, zero rows allowed | one row per `ValidationFinding` | `SourceRowRef`, `RuleRef`, severity, predicate outcome, and plan-authored validation message. No cell values. |
| `RejectedRowsArtifact` | optional; present iff `terminal_rejection_count > 0` | one row per terminally rejected source row | Logical Scan output row payload plus `SourceRowRef`, `RuleRef` of the terminal rule, and rejection kind. |
| `DeduplicationReportArtifact` | always, zero rows allowed | one row per duplicate finding | Duplicate `SourceRowRef`, keep-first `SourceRowRef`, `RuleRef`, key count, and encoded key byte count. No key bytes and no cell values. |

The bundle plus its provenance is the only visibility boundary. No artifact
may be loaded as visible without the bundle transaction that contains all
of its present members.

### 8.2 ValidationFinding model

`ValidationFinding` is a row-level result, not a lifecycle event:

- `source_row_ref`: identifies the logical Scan output row.
- `rule_ref`: exact plan fingerprint + node + rule ordinal.
- `severity`: `"warning"` or `"error"`.
- `predicate_outcome`: `"false"` or `"null"`.
- `message`: exact plan-authored message bytes, `1..=MAX_VALIDATION_MESSAGE_BYTES`
  after trim.

Warning findings never create a rejected-row payload. Error findings are
terminal and may create at most one rejected-row payload for the same
source row.

### 8.3 RejectedRowsArtifact model

Rejected rows are terminal rows only. For each terminally rejected source
row the engine stores exactly one payload row:

```text
[logical Scan output schema fields, exact order]
+ input_kind          : Utf8,   nullable = false  // "asset" in E4-C0
+ input_id            : Utf8,   nullable = false  // canonical SourceAsset.id
+ source_row_ordinal  : UInt64, nullable = false
+ rejection_kind      : Utf8,   nullable = false  // "validation_error" | "duplicate"
+ plan_fingerprint    : Utf8,   nullable = false  // canonical lowercase hex
+ node_id             : Utf8,   nullable = false  // terminal rule node
+ rule_ordinal        : UInt32, nullable = false  // terminal rule ordinal
```

Rules:

- Original payload fields preserve the logical Scan output schema field
  order, `ColumnId`, names, `LogicalType`, nullability, metadata, and Arrow
  values exactly once per rejected source row.
- Warning-only rows never appear here.
- A source row can be terminally rejected at most once because the first
  terminal rule stops processing. Therefore at most one original-row payload
  exists per source row; there is no 256-copy amplification.
- Zero terminal rejections means the artifact is absent. No empty
  `DatasetSnapshot` or empty snapshot-like manifest is created for rejected
  rows.
- Rejection kind `"duplicate"` points to the first `Deduplicate` rule that
  fired for that row. Rejection kind `"validation_error"` points to the
  first Error `Validate` rule that fired.

### 8.4 ValidationReportArtifact schema

```text
input_kind          : Utf8,   nullable = false
input_id            : Utf8,   nullable = false
source_row_ordinal  : UInt64, nullable = false
plan_fingerprint    : Utf8,   nullable = false
node_id             : Utf8,   nullable = false
rule_ordinal        : UInt32, nullable = false
severity            : Utf8,   nullable = false  // "warning" | "error"
predicate_outcome   : Utf8,   nullable = false  // "false" | "null"
message             : Utf8,   nullable = false  // plan-authored message
```

Row order: ascending `source_row_ordinal`, then plan node order along the
linear path, then ascending `rule_ordinal`.

### 8.5 DeduplicationReportArtifact schema

```text
input_kind                 : Utf8,   nullable = false
input_id                   : Utf8,   nullable = false
source_row_ordinal         : UInt64, nullable = false  // duplicate row
first_source_row_ordinal   : UInt64, nullable = false  // keep-first row
plan_fingerprint           : Utf8,   nullable = false
node_id                    : Utf8,   nullable = false
rule_ordinal               : UInt32, nullable = false
key_column_count           : UInt32, nullable = false
encoded_key_byte_count     : UInt32, nullable = false
```

The report contains no key bytes and no original cell values. It references
the accepted keep-first row and the duplicate row; both are traceable to
their source rows.

### 8.6 Reserved control identities

The rejected rows control `ColumnId` values are fixed contract constants,
written in the later runtime crate and never generated at runtime:

```rust
pub const REJECTED_INPUT_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0011));
pub const REJECTED_INPUT_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0012));
pub const REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0013));
pub const REJECTED_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0014));
pub const REJECTED_PLAN_FINGERPRINT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0015));
pub const REJECTED_NODE_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0016));
pub const REJECTED_RULE_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0017));
```

Source schemas must not already contain these reserved names or ids; a
collision is preflight `InvalidPlan`. The rejected artifact schema has
`source_field_count + 7 <= MAX_SCHEMA_FIELDS`; a source schema with more
than `MAX_SCHEMA_FIELDS - 7` fields is preflight `InvalidPlan`.

### 8.7 Original-value preservation decision

- The original value copied into a rejected row is the logical Scan output
  row after projection and after `Scan.predicate`, before any `ApplyRules`.
- Columns omitted by `Scan.projection` were never read (or already dropped
  by the connector) and are intentionally not recovered.
- Values produced by earlier Derive/Cast/Replace rules are not copied into
  rejected rows. The accepted row is removed on terminal rejection, so
  those derived values exist only transiently inside the bounded engine
  payloads and are reconstructible by replaying the deterministic plan over
  the preserved source row.
- This fixed-schema choice keeps one rejected schema for all terminal
  rejections and preserves source `Schema`, `ColumnId`, nullability, and
  physical Arrow values exactly once per source row.

## 9. SQLite temporary dedup index

### 9.1 Ownership and open protocol

`stillflow-storage` proposes an owned `DedupIndex` handle. Opening is
exclusive and never deletes an existing file:

1. Path is `dedup_{run_id}.sqlite` under the storage-managed temp root.
   `run_id` is caller-injected and non-nil.
2. The storage crate creates the temp directory with permissions `0700`
   (Unix) and opens the database file with `create_new(true)` semantics.
   If any file already exists at that path, open fails with
   `StorageError::AlreadyExists`; the engine aborts the bundle and no file
   is removed.
3. After SQLite opens, the handle writes an ownership lease row containing
   the caller-injected `run_id` and `started_at` in the same database.
   The lease is advisory recovery metadata; exclusive `create_new` remains
   the primary ownership guard.
4. The handle sets `PRAGMA page_size = 4096`,
   `PRAGMA max_page_count = MAX_DEDUP_INDEX_PAGES`,
   `PRAGMA cache_size = -512`, and `PRAGMA journal_mode = DELETE`.
5. File permission is `0600` (Unix). On platforms without Unix modes, the
   storage crate applies the strongest equivalent owner-only ACL available
   and records that behavior in its tests.

### 9.2 Dedup table

```sql
CREATE TABLE dedup_index (
    node_id       BLOB    NOT NULL,
    rule_ordinal  INTEGER NOT NULL,
    key_bytes     BLOB    NOT NULL,
    PRIMARY KEY (node_id, rule_ordinal, key_bytes)
) WITHOUT ROWID;

CREATE TABLE dedup_lease (
    run_id          BLOB NOT NULL PRIMARY KEY,
    started_at_utc  TEXT NOT NULL
) WITHOUT ROWID;
```

- `node_id` is the exact 16-byte `Uuid` of `PlanNodeId`.
- `rule_ordinal` is zero-based within the containing `ApplyRules` node.
- Insert decision: `INSERT INTO dedup_index (...) VALUES (...)
  ON CONFLICT DO NOTHING`; `changes() == 1` means first occurrence,
  `0` means duplicate. The engine never computes a hash to decide.
- SQLite BLOB primary-key equality over full canonical bytes is the only
  equality path. No hash-only or approximate path exists.

### 9.3 Close and recovery

- `DedupIndex::close_and_delete(self) -> Result<(), StorageError>` is the
  explicit contract. The engine calls it after the last dedup insert and
  **before** `VerificationBundleWriter::commit`. If deletion fails, the
  bundle is aborted and no artifact becomes visible.
- `Drop` is defense-in-depth only: it attempts best-effort deletion but its
  errors are not a substitute for `close_and_delete`.
- No open path ever deletes a pre-existing file. Stale files are removed
  only by storage recovery, and only when the lease `started_at_utc` is
  older than the caller-supplied recovery cutoff. Active or unexpired lease
  files are never deleted.
- A process crash leaves a stale temp file. The crashed run cannot have
  committed its bundle because index cleanup precedes commit; storage
  recovery later removes the expired lease file. Retry with the same
  `run_id` before recovery returns `AlreadyExists`; after recovery it
  starts from an empty index.
- The dedup index is per-run and disposable. It is never a persisted
  dataset and can never change an already committed bundle.

### 9.4 Resource caps

| Property | Frozen value |
| --- | --- |
| SQLite page size | 4,096 bytes |
| `PRAGMA max_page_count` | `MAX_DEDUP_INDEX_PAGES` = 2,097,152 pages (8 GiB) |
| Disk ceiling | `MAX_DEDUP_INDEX_DISK_BYTES` = 8 GiB per run |
| Cache memory | `PRAGMA cache_size = -512` (512 KiB), counted in operator state |
| Index rows per `(node_id, rule_ordinal)` | at most `MAX_SNAPSHOT_ROWS` |
| Total index rows per run | at most `MAX_SNAPSHOT_ROWS` across all namespaces |
| Temp directory / file modes | `0700` / `0600` (Unix) |

## 10. Atomic publication and security

### 10.1 VerificationBundle transaction

Publication sequence for `materialize_verification`:

1. Apply the default deadline and acquire the existing E2 run-gate permit
   (`try_acquire`, never await).
2. Run shared E2 preflight with E4 target disabled and the E4 rule checks.
3. Validate injected identities, input ref, provenance fields, and artifact
   id uniqueness.
4. `SnapshotStore::begin_verification_bundle(draft, started_at)`. This
   acquires exactly one storage publisher permit and creates one bundle
   staging context for all present and potential artifacts.
5. `SnapshotStore::open_dedup_index(run_id, started_at)`.
6. Open the connector stream exactly once and process accepted rows,
   validation findings, rejected payloads, and duplicate findings.
7. Call `DedupIndex::close_and_delete()`.
8. `VerificationBundleWriter::commit(committed_at)`, which makes accepted
   snapshot, validation report, optional rejected artifact, deduplication
   report, and bundle provenance visible in one SQLite transaction.
9. Return `VerificationBundle`.

The commit is the only visibility point. A reader either loads the complete
bundle by `run_id` (or by accepted snapshot id) or sees none of it. There
is no API that loads validation report, rejected rows, or deduplication
report independently of the bundle transaction.

### 10.2 Zero-rejection rule

If `terminal_rejection_count == 0`, the writer commits no rejected rows
artifact and creates no empty `DatasetSnapshot` / snapshot manifest for it.
`VerificationBundle.rejected_rows` is `None`. Validation and deduplication
reports may be zero-row artifacts and remain present so callers always have
one uniform report shape.

### 10.3 Failure and cancellation

Cancellation and deadline are observed:

1. before preflight inspect I/O;
2. before opening `read_batches`;
3. on every connector stream poll;
4. before lowering each connector envelope;
5. before every accepted writer append;
6. before every report/rejected writer append;
7. before `DedupIndex::close_and_delete`;
8. before `VerificationBundleWriter::commit`.

A cancelled or timed-out run returns `Cancelled` / `Timeout`, publishes no
bundle, and leaves no partial partition, report file, or temp index. A
storage error during bundle commit rolls back every manifest row and every
installed partition directory. Tests must assert that loading the bundle by
`run_id` fails after every failure injection and that no child artifact is
independently visible.

Dropping an uncommitted `VerificationBundleWriter` aborts the whole bundle
staging context. `DedupIndex::Drop` remains best-effort and is followed by
recovery as needed.

### 10.4 Identity and timestamp injection

| Field | Source |
| --- | --- |
| `run_id` | `identities.run_id` |
| Accepted snapshot id / dataset id | `identities.snapshot_id` / `identities.dataset_id` |
| Validation report artifact id | `identities.validation_report_id` |
| Rejected rows artifact id | `identities.rejected_rows_id` (used only if present) |
| Deduplication report artifact id | `identities.deduplication_report_id` |
| `session_id` | `identities.session_id` for accepted snapshot and bundle provenance |
| `source_asset_id` | bound `SourceAsset.id` |
| `input_ref` | `InputRef::Asset { asset_id: bound SourceAsset.id }` |
| `lineage` | `identities.lineage` |
| Accepted `quality_score` | `identities.quality_score` |
| `created_at` / `started_at` / `committed_at` | `identities.created_at` / `identities.started_at` / `identities.committed_at` |
| Dedup temp file identity | `identities.run_id` and `identities.started_at` |

The engine must not call `Uuid::new_v4` or `Utc::now` for any published
identity or timestamp. Wall-clock `Instant` remains allowed only for
deadline observation. Nil ids, nil lineage ids, quality scores above 100,
duplicate artifact ids, and timestamp order violations are rejected before
`begin_verification_bundle`.

### 10.5 Security boundary for raw values

Raw source cell values and derived failing values may exist only in:

- the bounded connector/Polars/remainder payloads during execution;
- accepted Parquet partitions (for accepted rows);
- rejected Parquet partitions (for terminal rejected rows);
- the disposable SQLite dedup index as canonical key bytes.

They must never appear in:

- `EngineError` `Display` / `Debug`;
- `sanitized_summary().message()`;
- logs, tracing fields, or event metadata;
- `ValidationReportArtifact` or `DeduplicationReportArtifact`;
- artifact provenance, summaries, or manifests.

The sanitization sentinel remains the UTF-8 string
`STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a`. It must appear as a cell value in
failing fixtures and must not appear in any `EngineError` surface or
serialized sanitized summary/event metadata.

### 10.6 Validation message safety

- The plan-authored validation message is report data, not a log. It is
  stored only in `ValidationReportArtifact.message`.
- E4 preflight explicitly performs all of the following and does **not**
  rely on the indirect `Rule::validate` path:
  1. trim and require non-empty;
  2. enforce `1..=MAX_VALIDATION_MESSAGE_BYTES` UTF-8 bytes after trim;
  3. call `ensure_no_secret_fields(&serde_json::Value::String(message.clone()))`
     and map failure to `EngineError::InvalidPlan`;
  4. call `Expr::Literal(ScalarValue::Utf8(message.clone())).validate_shape()`
     for literal-shape validation.
- The engine must not put message text in `EngineError`, Debug, logs, or
  future E5 events. Future E5 events may carry counts, `RuleRef`,
  severity, and `SourceRowRef` ranges only.
- `run_id`, artifact ids, `plan_fingerprint`, `node_id`, `rule_ordinal`,
  severity, row ordinal, batch sequence, and resource counts are safe
  correlation metadata and may appear in sanitized errors.

### 10.7 Error surface

E4 uses the existing `EngineError` variants only:

- bad Validate/Dedup shape, provenance/identity shape, or reserved-name
  collision: `InvalidPlan`;
- unknown key column: `UnknownColumn`;
- non-Boolean Validate predicate or paused key type: `TypeError`;
- key/row/message/disk/memory/page-count cap exceeded: `BoundExceeded`;
- SQLite index or bundle storage failures: `Storage(inner)`;
- all other E2 categories unchanged.

`EngineError` remains non-`Serialize`; only `SanitizedErrorSummary` crosses
a public boundary.

## 11. Proposed public API

Names may be organized into modules. Semantics, field order, and limits
must match this section. This docs PR adds no Rust code.

```rust
pub const VERIFICATION_CONTRACT_VERSION: u16 = 1;
pub const VERIFICATION_MAX_LIVE_COLUMNAR_PAYLOADS: u8 = 6;
pub const VERIFICATION_MAX_ENGINE_PEAK_BYTES: usize =
    (4 * MAX_BATCH_BYTES) + (2 * REPORT_PACK_BYTES) + MAX_OPERATOR_STATE_BYTES;
    // 4 * 64 MiB + 2 * 2 MiB + 5 MiB = 265 MiB

pub const MAX_DEDUP_KEY_COLUMNS: usize = 64;
pub const MAX_DEDUP_KEY_BYTES: usize = 64 * 1024;
pub const MAX_DEDUP_INDEX_CACHE_BYTES: usize = 512 * 1024;
pub const MAX_DEDUP_INDEX_PAGES: u32 = 2_097_152; // 8 GiB at 4096-byte pages
pub const MAX_DEDUP_INDEX_DISK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const MAX_VALIDATION_FINDINGS_PER_ROW: usize = MAX_RULES_PER_NODE; // 256
pub const MAX_VALIDATION_FINDINGS_PER_RUN: u64 = MAX_SNAPSHOT_ROWS as u64;
pub const MAX_VALIDATION_MESSAGE_BYTES: usize = 1_024;
pub const REPORT_PACK_ROWS: usize = 1_024;
pub const REPORT_PACK_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_REPORT_REMAINDER_BYTES: usize = REPORT_PACK_BYTES;
pub const VERIFICATION_MAX_COMPILED_PLAN_BYTES: usize = 3 * 1024 * 1024;
pub const VERIFICATION_MAX_ROUTING_STATE_BYTES: usize = 512 * 1024;

pub struct VerificationIdentities {
    pub run_id: Uuid,
    pub snapshot_id: Uuid,
    pub dataset_id: Uuid,
    pub validation_report_id: Uuid,
    pub rejected_rows_id: Uuid,
    pub deduplication_report_id: Uuid,
    pub session_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub committed_at: DateTime<Utc>,
    pub lineage: BTreeSet<Uuid>,
    pub quality_score: Option<u8>,
}

pub struct VerificationRequest<'a> {
    pub plan: LogicalPlan,
    pub connection: SourceConnection,
    pub asset: SourceAsset,
    pub schema_override: Option<LogicalSchema>,
    pub identities: VerificationIdentities,
    pub context: RequestContext,
    pub batch_size: usize,
    pub store: &'a SnapshotStore,
}

pub struct VerificationBundleDraft {
    pub provenance: ArtifactProvenance,
    pub accepted: SnapshotDraft,
    pub validation_report_id: Uuid,
    pub rejected_rows_id: Uuid,
    pub deduplication_report_id: Uuid,
}

pub struct VerificationBundle {
    pub provenance: ArtifactProvenance,
    pub accepted: AcceptedSnapshotArtifact,
    pub validation_report: ValidationReportArtifact,
    pub rejected_rows: Option<RejectedRowsArtifact>,
    pub deduplication_report: DeduplicationReportArtifact,
}

pub struct AcceptedSnapshotArtifact {
    pub manifest: SnapshotManifest,
    pub provenance: ArtifactProvenance,
}

pub struct ValidationReportArtifact {
    pub manifest: ArtifactManifest,
    pub provenance: ArtifactProvenance,
}

pub struct RejectedRowsArtifact {
    pub manifest: ArtifactManifest,
    pub provenance: ArtifactProvenance,
}

pub struct DeduplicationReportArtifact {
    pub manifest: ArtifactManifest,
    pub provenance: ArtifactProvenance,
}

impl ExecutionEngine {
    pub async fn materialize_verification(
        &self,
        request: VerificationRequest<'_>,
    ) -> Result<VerificationBundle, EngineError>;
}

impl SnapshotStore {
    pub fn begin_verification_bundle(
        &self,
        draft: VerificationBundleDraft,
        started_at: DateTime<Utc>,
    ) -> Result<VerificationBundleWriter, StorageError>;

    pub fn load_verification_bundle(
        &self,
        run_id: Uuid,
    ) -> Result<VerificationBundle, StorageError>;

    pub fn load_verification_bundle_by_snapshot(
        &self,
        snapshot_id: Uuid,
    ) -> Result<VerificationBundle, StorageError>;

    pub fn open_dedup_index(
        &self,
        run_id: Uuid,
        started_at: DateTime<Utc>,
    ) -> Result<DedupIndex, StorageError>;
}

impl VerificationBundleWriter {
    pub fn append_accepted(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn append_validation_findings(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn append_rejected_rows(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn append_duplicate_findings(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn commit(self, committed_at: DateTime<Utc>) -> Result<VerificationBundle, StorageError>;
}

impl DedupIndex {
    pub fn insert_first(
        &self,
        node_id: Uuid,
        rule_ordinal: u32,
        key_bytes: &[u8],
    ) -> Result<bool, StorageError>; // true = first occurrence

    pub fn close_and_delete(self) -> Result<(), StorageError>;
}
```

Compatibility note: every type above is outside the current E2/E3 public
surface and is `Proposed`. If PR #53 changes `PreviewRequest` /
`PreviewResult` before E4 runtime starts, E4 must reconcile only section 13
against the merged API; it must not silently alter E3 fields.

## 12. Memory model and bounded state

### 12.1 Verification memory law

The E2 `materialize` law (three columnar payloads, 197 MiB) is unchanged.
The new `materialize_verification` path may keep at most six bounded live
payloads:

```text
connector envelope             <= MAX_BATCH_BYTES           (64 MiB)
complete Polars working set    <= MAX_BATCH_BYTES           (64 MiB)
accepted canonical remainder   <= MAX_BATCH_BYTES           (64 MiB)
rejected canonical remainder   <= MAX_BATCH_BYTES           (64 MiB)
validation report remainder    <= REPORT_PACK_BYTES         (2 MiB)
dedup report remainder         <= REPORT_PACK_BYTES         (2 MiB)
bounded non-columnar state     <= MAX_OPERATOR_STATE_BYTES  (5 MiB)
peak                            = 4 * 64 MiB + 2 * 2 MiB + 5 MiB
                                = 265 MiB
```

Accepted and rejected remainders use the E2 move/freeze rule. Report
remainders are bounded by `REPORT_PACK_ROWS = 1_024` and
`REPORT_PACK_BYTES = 2 MiB`; a single report row is far below that bound by
construction. Flushing a remainder into an output envelope moves the
allocation; no seventh payload and no second `MAX_BATCH_BYTES`-class copy
is authorized.

Report canonical rebatching uses the same deterministic algorithm as E2
section 14.4, with report-specific `pack_limit = REPORT_PACK_ROWS` and byte
cap `REPORT_PACK_BYTES`. Fixed report pack limits make report envelope
boundaries independent of the user `batch_size` and connector partitions.

### 12.2 Operator-state budget

`MAX_OPERATOR_STATE_BYTES` remains 5 MiB. The verification path allocates
within it:

| Component | Ceiling |
| --- | --- |
| Verification compiled plan | `VERIFICATION_MAX_COMPILED_PLAN_BYTES` = 3 MiB |
| FFI scratch | `MAX_FFI_SCRATCH_BYTES` = 1 MiB |
| SQLite dedup cache | `MAX_DEDUP_INDEX_CACHE_BYTES` = 512 KiB |
| Routing metadata (ordinals, masks, counters, finding buffers) | `VERIFICATION_MAX_ROUTING_STATE_BYTES` = 512 KiB |

The law is the actual sum, not the ceilings added:
`actual_compiled + actual_ffi + actual_dedup_cache + actual_routing <= 5 MiB`.
If a plan would exceed `VERIFICATION_MAX_COMPILED_PLAN_BYTES`, or the
measured sum would exceed 5 MiB, the run returns `BoundExceeded` before
bundle commit. The dedup index file is disk state and is not engine memory
beyond its configured 512 KiB SQLite page cache.

### 12.3 No unbounded in-memory dedup

- `HashSet`, `HashMap`, Bloom filters, sketches, digest-only caches, and
  per-batch duplicate maps are forbidden for the keep-first decision.
- Source grep alone is not sufficient. Tests must instrument the engine
  allocator and SQLite cache and prove that dedup state does not grow with
  distinct keys beyond the configured cache and the routing-state ceiling.
- Approximate deduplication (locality-sensitive hashing, n-grams, fuzzy
  matching) is forbidden.

## 13. Preview relationship

E4-C0-R1 does **not** extend `PreviewResult`, `PreviewRequest`, or the E3
preview execution path.

- The E3 preview contract in Issue #50 (and its current PR #53 revision)
  remains the only Preview semantics. Plans containing
  `Rule::Validate` / `Rule::Deduplicate` continue to return
  `UnsupportedRule` from the preview path until a future E4-P contract
  explicitly changes that.
- E4 runtime must not implement a second Validate/Deduplicate Preview
  path, must not return partial verification artifacts from Preview, and
  must not duplicate E4 routing logic for Preview.
- No public Preview field changes are authorized here. If a future Preview
  revision needs verification preview fields, it must be a separate
  approved contract and must reuse the exact row identity, Validate
  true/false/null, canonical key equality, and routing rules frozen in this
  document.

## 14. Resource ceilings

| Resource | Ceiling | Source |
| --- | --- | --- |
| Verification live payloads | 6 | this contract |
| Verification peak engine bytes | 265 MiB | `4 * 64 MiB + 2 * 2 MiB + 5 MiB` |
| Operator state | 5 MiB | E2, shared budget in section 12.2 |
| Verification compiled plan | 3 MiB | this contract |
| Dedup SQLite cache | 512 KiB | this contract |
| Routing metadata | 512 KiB | this contract |
| Report pack rows / bytes | 1,024 / 2 MiB | this contract |
| Report remainder bytes | 2 MiB each | this contract |
| Dedup key columns per rule | 64 | this contract |
| Encoded composite dedup key | 64 KiB | this contract |
| Dedup index disk | 8 GiB | `PRAGMA max_page_count` = 2,097,152 |
| Dedup index rows per run | `MAX_SNAPSHOT_ROWS` (1,000,000,000) | storage |
| Validation findings per source row | 256 | this contract |
| Validation findings per run | 1,000,000,000 | this contract |
| Validation message | 1,024 UTF-8 bytes after trim | this contract |
| Source row ordinal domain | `0..MAX_SNAPSHOT_ROWS` | this contract |
| Rejected payload rows | at most source rows; no amplification | this contract |
| Rejected artifact rows / bytes / partitions | `MAX_SNAPSHOT_ROWS` / 1 TiB / `MAX_SNAPSHOT_PARTITIONS` | storage |
| Accepted snapshot limits | unchanged E2 | storage |
| Input envelope rows / bytes | 65,536 / 64 MiB | core |
| `batch_size` | `1..=65_536` | E2 / `ReadRequest` |
| Plan nodes / rules per node / expr nodes / depth | 64 / 256 / 1,024 / 64 | E2 |
| Engine concurrent runs | `MAX_ENGINE_CONCURRENT_RUNS` = 4 | E2 run gate |
| Storage publishers | one publisher permit per verification bundle; max 8 | storage |
| Default / maximum deadline | 15 min / 30 min | E2 |

Exceeding any ceiling is a typed error before visible publication. No TBD
value is permitted.

## 15. Determinism, partition invariance, and retry

A `materialize_verification` run is deterministic when all of the following
hold:

1. Identical authorized source rows and order, identical validated plan,
   identical `batch_size`, and identical injected identities produce:
   - identical accepted logical rows, schema, and envelope boundaries;
   - identical validation findings and report envelope boundaries;
   - identical rejected rows and envelope boundaries (or identical `None`);
   - identical duplicate findings and report envelope boundaries;
   - identical artifact summaries and provenance bytes except
     caller-injected timestamps, which are required to be identical.
2. Changing only connector batch partitioning must not change which rows
   are accepted or rejected, their order, their schemas, the stats, or the
   envelope boundaries of any artifact. `source_row_ordinal` follows
   logical Scan output order, never physical partitions.
3. Changing only the layer that evaluates `Scan.predicate` (pushed down or
   in-engine) must not change `source_row_ordinal`, findings, or artifacts,
   because the ordinal domain starts after logical Scan output.
4. Deduplicate decisions depend only on `(node_id, rule_ordinal,
   canonical_key_bytes, source_row_ordinal)` and SQLite BLOB equality, not
   on hash iteration, HashMap order, locale, clock, process id, or Polars
   approximate duplicate routines.
5. Injected `created_at` / `started_at` / `committed_at` are the only
   timestamps written into storage calls. `Instant` is used only for
   deadline observation.
6. Plan canonical bytes and fingerprints remain `stillflow-plan` values;
   E4 must not invent a second fingerprint.

Retry law:

- A retry after an aborted/failed attempt uses a fresh or recovered dedup
  index. `open_dedup_index` never deletes a pre-existing file; if the same
  `run_id` still has an unexpired lease, the retry fails with
  `StorageError::AlreadyExists`. The caller either runs storage recovery
  or supplies a new `run_id`.
- After recovery or with a fresh `run_id`, identical inputs and identical
  caller-injected identities produce identical rows, findings, order,
  stats, and partition boundaries.
- No committed bundle is modified or recomputed by retry.

## 16. Acceptance matrix

The sanitization sentinel is
`STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a`.

| ID | Criterion | Automated evidence |
| --- | --- | --- |
| V01 | Validate true passes; false Warning keeps accepted row and emits only a warning finding; false Error rejects; null is failure | Fixture per severity with true/false/null predicate columns; accepted rows, validation findings, rejected payloads, and summaries match. Null predicate is never a pass. |
| V02 | All pass, all terminal-reject, and empty source | Accepted/report/rejected presence and counts match; empty source yields accepted snapshot plus two zero-row reports and `rejected_rows = None`. |
| V03 | Cross-batch global dedup keep-first | Distinct keys span at least three execution chunks/envelopes; only the lowest `source_row_ordinal` per key is accepted; each later row produces one rejected payload and one duplicate finding. |
| V04 | Connector partition invariance | Two partitionings of the same ordered rows produce identical accepted rows, validation findings, rejected rows, duplicate findings, schemas, summaries, and envelope boundaries. |
| V05 | Null, NaN, `-0.0`/`+0.0` key equality | Null duplicates null; all NaN bit patterns group together; `-0.0` and `+0.0` are duplicates; finite distinct floats remain distinct. |
| V06 | Multiple Validate hits and one-payload guarantee | Warning then Error on one row emits both findings in rule order; first Error terminates later rules; rejected artifact contains exactly one payload for that row; 256-finding cap fails with `BoundExceeded` and no bundle. |
| V07 | Warning rows never enter rejected artifact | Warning-only fixture has zero terminal rejections, `rejected_rows = None`, and warning finding references the accepted row. |
| V08 | Duplicate rows enter rejected artifact and dedup report, not silent deletion | Each duplicate has one payload with `rejection_kind = "duplicate"`, one `DuplicateFinding` with keep-first `SourceRowRef`, and no key bytes in the report. |
| V09 | Cancellation and deadline publish nothing | Inject cancel/deadline at each section 10.3 point; `load_verification_bundle(run_id)` fails; temp index absent or recoverable; no staging residue. |
| V10 | Bundle atomicity | Inject failure during commit after accepted partition install; neither accepted snapshot nor any report/rejected artifact is independently visible; rollback cleans all files. |
| V11 | Zero-rejection rule | No rejected artifact row is inserted when terminal rejection count is zero; storage creates no empty DatasetSnapshot for rejected rows. |
| V12 | Dedup index ownership and recovery | `create_new` fails with `AlreadyExists` for an existing file and deletes nothing; `close_and_delete` removes file; crash-stale file with expired lease is removed only by recovery; active lease is never removed. |
| V13 | Dedup index permissions and page cap | Temp dir mode `0700`, DB file mode `0600`; `PRAGMA max_page_count` equals `MAX_DEDUP_INDEX_PAGES`; disk > 8 GiB fails `BoundExceeded`. |
| V14 | Memory ceiling | Instrumented live-payload counter shows `<= 6` and no seventh payload; allocator/SQLite cache stay within section 12.2; source grep and allocator prove no in-memory `HashSet`/`HashMap` dedup index. |
| V15 | Secret sentinel | Sentinel appears in a failing cell but not in `EngineError` Display/Debug, `sanitized_summary()` JSON, event metadata, reports, or provenance summaries. |
| V16 | Retry determinism | Retry after recovery/fresh run id with identical inputs/identities produces identical artifacts and partition boundaries; pre-existing active dedup file is never deleted. |
| V17 | Utf8 and Binary key equality | Exact byte equality; empty string/binary distinct from null; no normalization/collation. |
| V18 | Timestamp key boundary and equality | Millisecond/Microsecond/Nanosecond same-type same-epoch duplicates; `Timestamp { unit: Second }`, `List`, and `Struct` keys are preflight `TypeError`. |
| V19 | Canonical key bytes and collision safety | Golden vectors for every supported tag, including `timezone: None` vs `Some` presence encoding; different values never produce equal bytes; SQLite BLOB PK is the only duplicate decision path. |
| V20 | Key bounds | 65th key column, encoded key > 64 KiB, and SQLite `max_page_count` disk cap each fail `BoundExceeded` before bundle commit with no visible artifact. |
| V21 | Schema/ColumnId/original value preservation | Rejected schema field order and metadata match logical Scan output + seven control fields; ColumnIds unchanged; Arrow values equal source values including null/NaN/`-0.0`; at most one payload per source row. |
| V22 | Validation message safety and length | Explicit E4 preflight rejects empty-after-trim, > 1,024 bytes, and secret-like message; exact safe message stored only in validation report; absent from errors/logs/events. |
| V23 | Existing E2/E3 compatibility | `materialize` still returns `UnsupportedRule` for Validate/Deduplicate; `preview` behavior is unchanged by the E4 code path; no `PreviewResult` field changed. |
| V24 | Provenance completeness and CI | Every artifact embeds `ArtifactProvenance` with `run_id`, `InputRef`, `plan_fingerprint`, versions, lineage, injected times, and sanitized summary; `RuleRef` contains plan fingerprint + node + rule ordinal. CI checks pass in the later runtime PR; this docs PR modifies only the file named in section 2. |

## 17. Stop conditions

Stop and return to contract review if implementation needs:

- a public type or field not named `Proposed` in section 11;
- a change to `PreviewRequest` / `PreviewResult` or a second Preview
  Validate/Deduplicate semantics;
- a warning row or duplicate finding stored as a rejected original-row
  payload, or more than one rejected payload per source row;
- a rejected artifact created for zero rejections;
- artifact visibility outside the `VerificationBundle` transaction;
- an in-memory `HashSet`/`HashMap` duplicate index or hash-only decision;
- approximate/fuzzy deduplication;
- a new cleaning rule language in DuckDB, SQL, or Python;
- unbounded collect, prefetch, full-source materialization, or temp files
  outside the storage-managed temp root;
- blind deletion of an existing dedup index file;
- generated snapshot/dataset/session/run/artifact ids or timestamps;
- a message or cell value in `EngineError`, Debug, logs, events, or
  sanitized summaries;
- serializing `EngineError`;
- Dependabot or unrelated lockfile edits;
- Join/Union execution or a third runtime path.

## 18. Known risks

- The verification path keeps six bounded live payloads and has a 265 MiB
  peak. A seventh payload or a second `MAX_BATCH_BYTES`-class copy is
  `Internal`.
- Report pack limits are fixed at 1,024 rows / 2 MiB. Validation message
  length is capped at 1 KiB, so one report row always fits; future message
  expansion requires a new contract and a new report byte proof.
- SQLite full-BLOB equality is exact but the temp index is large. The page
  cap must be enforced by `PRAGMA max_page_count`, not estimated after the
  fact.
- `open_dedup_index` deliberately returns `AlreadyExists` for any existing
  same-`run_id` file. This trades retry convenience for active-file safety;
  recovery must be called for stale leases.
- `source_row_ordinal` starts after logical Scan output, so rows dropped by
  `Scan.predicate` are not referenceable. This is the R1 identity domain;
  changing it later is a breaking contract.
- The reserved seven rejected-control fields reduce the maximum cleanable
  source schema from 4,096 to 4,089 fields. This is frozen, not dynamic.
- Timestamp equality is type-local and E4-C0 excludes Second/List/Struct.
  Any future expansion must not reuse the reserved tags without a new
  approved contract.
- PR #53 may still revise the E3 public surface. E4 runtime must not start
  until PR #53 merges and must reconcile only section 13 against the merged
  API.
