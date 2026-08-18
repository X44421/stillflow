# Issue #54 Implementation Contract: validation, rejected rows, and exact deduplication (E4-C0-R4)

> Status: Frozen for architecture review (not approved)
> Revision: C0-R4
> Supersedes: C0-R3 `cf4f0bdd7207c0a961d05e56ac69bf26578b42da` (Request changes)
> Also supersedes: C0-R2 `2a35bced9e2eb8b35a9e4679c8698d15bbb6b941` (Request changes)
> Also supersedes: C0-R1 `e5b70db4bfdfd9546842c138d50ec815440725fc`
> Also supersedes: C0 `d33f45610620c03afe253cdc0b4aef7468fa5dd8`
> Risk: High
> Issue: #54 (contract)
> PR: #57 draft (actual PR number; C0 header said expected #55 and is corrected here)
> Parent: Issue #46 revision R3, merged at
> `32f1c53d9903f66aeaca1c2676c0b81abfb2a702` in PR #47
> Authorized E4 base: `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`
> Storage facts base: `main@473c65b` (PR #62 merged storage publication/recovery inventory)
> Branch: `agent/issue-054-validation-rejected-rows-contract`
> Last updated: 2026-08-18 (R4 after Request changes)
> Review: PR #57 is Ready with Request changes. C0, C0-R1, and C0-R2 were not
> approved. R3 closed the previous identity, recovery, digest, resource-scope,
> and acceptance-executability blockers. R4 closes the remaining report
> `ColumnId`, canonical digest/encoding, journal-before-staging recovery, and
> SQLite initialization/memory-limit blockers.
> Architecture approval binds exactly one new commit SHA of this file. E4
> runtime remains paused until that approval and must then rebuild from the
> latest accepted `main`.

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

- The authorized E4 base is `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`.
- This branch is created from that exact commit. It must not merge, rebase
  onto, or cherry-pick from PR #53, PR #49, or any historical branch.
- The docs-only storage inventory PR #62 was merged into `main@473c65b` and is
  merged into this branch solely to bind E4 bundle publication/recovery to the
  verified storage facts in
  `docs/issues/storage-publication-recovery-inventory.md`. It adds no runtime
  code and does not alter the E4 contract's non-goals.
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
| Fact error: claim that `Rule::validate` already performs `ensure_no_secret_fields` for the validation message | R1 requires an explicit E4 preflight security check and does not rely on the indirect `Rule::validate` path (section 10.7). |

### 3.2 R1 implementation blockers closed by R2

| R1 P0 | R2 disposition |
| --- | --- |
| Storage type ownership not frozen; `ArtifactManifest` undefined; no report/rejected reader | Added crate ownership table in section 7.1, full `ArtifactManifest` / `ArtifactSection` / `ArtifactPartition` definitions in section 8.1, bundle membership, and bounded `ArtifactBatchReader` through bundle APIs. |
| Provenance not constructible/auditable; draft carried final summary; `session_id` missing; `artifact_id = run_id`; no input/plan integrity digest | Split `ArtifactProvenanceDraft` from committed `ArtifactProvenance`; added `bundle_id`, `session_id`, `LogicalInputRef` with SHA-256, `canonical_plan_digest`, `engine_build`, writer-computed `summary` and `content_digest`; `PlanFingerprint` is index-only. |
| Reports are finding logs only; Dedup report cannot be generated; message repeated; no first ordinal | Added `ValidationRuleSummary` and `DedupRuleSummary`; message stored once per `RuleRef`; SQLite index stores `first_source_row_ordinal`; `insert_first()` returns typed `DedupInsert`; changed “accepted keep-first” wording to “first-seen at this rule”. |
| Resource/failure model not closed; report limits inconsistent; crash state machine missing; dedup recovery not gated | Made report row/byte/partition limits mathematically consistent in section 14; froze reserve-before-allocate and allocator phases; added bundle crash/recovery state machine in section 10; dedup recovery uses maintenance gate + OS lock in section 9. |

### 3.3 R3 blockers closed by this revision

| R2 blocker | R3 disposition |
| --- | --- |
| Dedup `.sqlite` / `.lock` creation could leave an orphan and recovery scanned only `.lock` | Frozen lock-first creation, exclusive-open rollback for files created by the current attempt, crash points after every creation step, and recovery over the union of `.sqlite` and `.lock` candidates (section 9). |
| `artifact_id` conflicted with `bundle_id`; bundle provenance and membership identities were undefined | Added a distinct `bundle_artifact_id`, separate accepted/child artifact identities, and renamed membership fields to the exact `ArtifactManifest.artifact_id` values (section 7 and section 8.1). |
| Manifest/content digest order and multi-section summaries were under-specified | Frozen byte-level manifest encoding, section/partition ordering, artifact and bundle digest formulas, summary aggregation, and report-limit scope (section 8.1.1). |
| V09 had no load-by-run-id API; cancellation cleanup and variable-width key bounds conflicted | Added `load_verification_bundle_by_run_id`, made normal cancellation cleanup strict, and limited preflight key-size proofs to statically bounded types while retaining the required per-row runtime check (sections 6, 10, 11, and 16). |
| R3 was not yet bound to the merged storage publication/recovery inventory | R3 now incorporates the PR #62 facts from `main@473c65b`: the publication journal commits before staging creation, final files precede SQLite visibility, and snapshot visibility plus journal deletion share one SQLite transaction. Bundle states reuse the existing storage maintenance gate/root lock and do not claim untested process-kill or power-loss durability. |

### 3.4 R4 blockers closed by this revision

| R3 blocker / review item | R4 disposition |
| --- | --- |
| Report schemas do not freeze `ColumnId`; runtime would generate report IDs | Added fixed `ColumnId` constants for every `ValidationRuleSummary`, `ValidationFinding`, `DedupRuleSummary`, and `DuplicateFinding` field (section 8.7). Runtime never generates report IDs. |
| Digest model references non-existent `LogicalSchema::canonical_bytes()`; `SnapshotManifest` has no version/digest; `canonical_batch_bytes` and accepted-snapshot provenance digest are not fully defined; `LogicalInputRef.version_digest` preimage is undefined | R4 freezes `canonical_schema_bytes` (including the required `LogicalSchema::canonical_bytes()` encoding), `canonical_batch_bytes` as Arrow IPC record-batch body, `LogicalInputRef.version_digest` preimage, and an explicit `accepted_snapshot_manifest_digest` formula over `DatasetSnapshot` + `SnapshotPartition` values (section 8.1.1). |
| Crash recovery state machine misses journal-before-staging window | `Prepared` now means the publication journal row is committed before staging; recovery removes that committed row. V30 injects this exact window (sections 10.4 and 16). |
| SQLite initialization writes lease before `PRAGMA page_size`, and `cache_size=-512` is described as a strict memory cap | R4 sets all PRAGMAs immediately after exclusive creation and before any table/lease row, and clarifies `cache_size` is a soft target, not a hard 512 KiB limit (section 9.1). |

### 3.5 Compatibility decision

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
- Numeric resource ceilings and the V01–V30 acceptance matrix.

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
  set of section 6.3. For fixed-width key components, preflight computes a
  static maximum encoded length. Utf8/Binary values are variable-width and
  therefore have no preflight proof of a finite row length under the current
  logical schema contract; they are admitted by preflight and bounded by the
  per-row runtime check below.
- Immediately before every SQLite insert, the engine computes the complete
  canonical key bytes and rejects an encoded length greater than
  `MAX_DEDUP_KEY_BYTES` with `EngineError::BoundExceeded`. This runtime check
  applies to fixed-width and variable-width keys alike and occurs before any
  SQLite write.
- Violations map to `UnknownColumn`, `TypeError`, or `BoundExceeded` as in
  section 10.8.
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

## 7. Unified artifact provenance and storage ownership

### 7.1 Crate ownership

Storage must never depend on `stillflow-plan` or `stillflow-engine`. The
provenance and artifact types are therefore split by dependency direction:

| Crate | Owns |
| --- | --- |
| `stillflow-core` | `InputRef`, `LogicalInputRef`, `SourceRowRef`, `RuleRef`, `ArtifactKind`, `ArtifactSummary`, caller-owned `ArtifactProvenanceInput`, engine-assembled `ArtifactProvenanceDraft`, committed `ArtifactProvenance` with `[u8;32]` digests, and `LogicalSchema::canonical_bytes()` (new E4 runtime API) |
| `stillflow-plan` | `PlanFingerprint`, `LogicalPlan::canonical_bytes()`, `PlanFingerprint::as_bytes()` |
| `stillflow-storage` | `ArtifactManifest`, `ArtifactSection`, `ArtifactPartition`, `VerificationBundleMembership`, `VerificationBundle`, `ArtifactBatchReader`, `DedupIndex`, bundle writer/commit/load APIs |
| `stillflow-engine` | `ExecutionEngine::materialize_verification`, conversion from `PlanFingerprint` / `PlanNodeId` to core `RuleRef` / digest bytes, and writer orchestration |

The dependency arrows remain:

```text
stillflow-api -> stillflow-engine
stillflow-engine -> stillflow-plan, stillflow-connectors, stillflow-storage
stillflow-plan -> stillflow-core
stillflow-connectors -> stillflow-core
stillflow-storage -> stillflow-core
stillflow-core -> no workspace crate
```

No storage type may name `PlanFingerprint`, `PlanNodeId`, `LogicalPlan`,
`ExecutionEngine`, or `stillflow-plan` types. Storage sees only core
`[u8;32]` digests and `Uuid` identities.

### 7.2 Provenance draft and committed provenance

The caller supplies only `ArtifactProvenanceInput`. The engine constructs
`ArtifactProvenanceDraft` before storage I/O by combining that input with the
verified canonical-plan digest and compile-time engine metadata. The draft
contains no summary and no content digest. The writer computes summary and
content digest during/after writing. This ownership split prevents callers from
forging engine-derived integrity fields or build identity.

```rust
pub enum InputRef {
    Asset { asset_id: Uuid },
    Snapshot { snapshot_id: Uuid },
}

pub struct LogicalInputRef {
    pub input: InputRef,
    /// Caller-injected SHA-256 over the versioned logical input descriptor
    /// (asset/snapshot identity + authorized schema/version), not raw rows.
    pub version_digest: [u8; 32],
}

pub struct SourceRowRef {
    pub input: LogicalInputRef,
    pub source_row_ordinal: u64,
}

pub struct RuleRef {
    /// SHA-256 of `LogicalPlan::canonical_bytes()`; integrity digest.
    pub canonical_plan_digest: [u8; 32],
    /// Existing non-security FNV-1a PlanFingerprint bytes; index only.
    pub plan_fingerprint: [u8; 32],
    pub node_id: Uuid,
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

pub type ContentDigest = [u8; 32];

pub struct ArtifactProvenanceInput {
    pub run_id: Uuid,
    pub bundle_id: Uuid,
    pub artifact_id: Uuid,
    pub artifact_kind: ArtifactKind,
    pub session_id: Uuid,
    pub input: LogicalInputRef,
    pub lineage: BTreeSet<Uuid>,
    pub created_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub committed_at: DateTime<Utc>,
}

pub struct ArtifactProvenanceDraft {
    pub input: ArtifactProvenanceInput,
    pub plan_fingerprint: [u8; 32],
    pub canonical_plan_digest: [u8; 32],
    pub engine_contract_version: u16,
    pub engine_build: String,
    pub verification_contract_version: u16,
}

pub struct ArtifactProvenance {
    pub draft: ArtifactProvenanceDraft,
    pub summary: ArtifactSummary,
    pub content_digest: [u8; 32],
}
```

### 7.3 Required provenance fields

| Field | Rule |
| --- | --- |
| `run_id` | Caller-injected non-nil `Uuid` in `ArtifactProvenanceInput`. Identifies one execution run. |
| `bundle_id` | Caller-injected non-nil `Uuid` in `ArtifactProvenanceInput`, independent from `run_id` and from every artifact id. One `VerificationBundle` has one `bundle_id`. |
| `artifact_id` | Caller-injected non-nil `Uuid` in `ArtifactProvenanceInput`, distinct from `bundle_id`, `run_id`, and every artifact sibling id. |
| `session_id` | Caller-injected non-nil `Uuid` in `ArtifactProvenanceInput`, present in the provenance structure and in the accepted snapshot draft. |
| `artifact_kind` | Caller-selected in `ArtifactProvenanceInput`, one of the five kinds. Bundle-level provenance uses `VerificationBundle`; child artifacts use their own kind. |
| `input` | Caller-injected `LogicalInputRef`. E4-C0 uses `InputRef::Asset { asset_id }` and requires it to equal the bound `SourceAsset.id`. `Snapshot` is reserved for future snapshot-input E4 revisions. |
| `canonical_plan_digest` | SHA-256 over `LogicalPlan::canonical_bytes()`, verified and inserted by the engine into the draft; the caller cannot supply or override it. |
| `plan_fingerprint` | Existing FNV-1a `PlanFingerprint::as_bytes()` copied into `[u8;32]` by the engine from the plan; it is an index only, not an integrity digest. |
| `engine_contract_version` | Existing `ENGINE_CONTRACT_VERSION`, inserted by the engine. |
| `engine_build` | Compile-time engine crate version string, inserted by the engine and never caller-generated. |
| `verification_contract_version` | `VERIFICATION_CONTRACT_VERSION = 1`, inserted by the engine. |
| `lineage` | Caller-injected `BTreeSet<Uuid>`; nil ids rejected. |
| `created_at` / `started_at` / `committed_at` | Caller-injected in `ArtifactProvenanceInput`; engine must not call `Utc::now`. Ordering checked as `created_at <= started_at <= committed_at`. |
| `summary` | Not in the draft. The writer computes it from actual written rows/partitions/findings. |
| `content_digest` | Not in the draft. The writer computes the exact SHA-256 formula in section 8.1.1 over the canonical manifest, ordered section digests, and provenance identity fields. |

The bundle-level provenance uses `ArtifactKind::VerificationBundle` and the
caller-injected `bundle_artifact_id`, which is distinct from `bundle_id`,
`run_id`, and every child artifact id. Every child artifact uses its own
artifact id. `VerificationBundleDraft` in section 11 carries the engine-built
bundle provenance draft plus the accepted `SnapshotDraft` and child artifact
ids; it is validated before any storage I/O.

`RuleRef` is stored in validation and deduplication report rows and in the
rejected rows control columns. It contains the canonical-plan SHA-256,
the FNV-1a index bytes, node id, and rule ordinal. This lets reports be
audited independently of `stillflow-plan` while preserving the existing
fingerprint as a fast lookup.

`SourceRowRef` is flattened into artifact rows as `input_kind`, `input_id`,
`input_version_digest`, and `source_row_ordinal`. E4-C0 writes
`input_kind = "asset"`, `input_id = SourceAsset.id`, and
`input_version_digest = identities.input.version_digest`.

## 8. VerificationBundle artifact model

### 8.1 ArtifactManifest, sections, and bundle membership

`ArtifactManifest` is the storage-owned manifest for report/rejected
artifacts. It is not `SnapshotManifest`; accepted data keeps the existing
`SnapshotManifest`.

```rust
pub struct ArtifactManifest {
    pub version: u16,
    pub artifact_id: Uuid,
    pub kind: ArtifactKind,
    pub sections: Vec<ArtifactSection>,
    pub manifest_digest: ContentDigest,
}

pub struct ArtifactSection {
    pub section_id: ArtifactSectionId,
    pub schema: LogicalSchema,
    pub schema_fingerprint: LogicalSchemaFingerprint,
    pub stats: ArtifactSectionStats,
    pub partitions: Vec<ArtifactPartition>,
    pub section_digest: ContentDigest,
}

pub struct ArtifactSectionStats {
    pub row_count: u64,
    pub stored_byte_count: u64,
    pub partition_count: u32,
}

pub struct ArtifactPartition {
    pub sequence: u32,
    pub row_count: u64,
    pub stored_byte_count: u64,
    pub digest: ContentDigest,
}
```

### 8.1.1 Canonical manifest, section, partition, and provenance digests

All digest inputs are versioned byte strings. Multi-byte integers use
little-endian encoding; UUIDs use their 16 bytes from `Uuid::as_bytes()`;
enum tags are fixed `u8` values; byte strings use a `u32` little-endian length
followed by the exact bytes; text is UTF-8 with no normalization. The digest
domain prefixes below are ASCII bytes followed by `0x00`:

| Digest | Domain prefix |
| --- | --- |
| partition | `stillflow.e4.partition.v1` |
| section | `stillflow.e4.section.v1` |
| manifest | `stillflow.e4.manifest.v1` |
| artifact provenance | `stillflow.e4.artifact-provenance.v1` |
| bundle provenance | `stillflow.e4.bundle-provenance.v1` |
| logical input | `stillflow.e4.logical-input.v1` |
| accepted snapshot | `stillflow.e4.accepted-snapshot.v1` |

The fixed enum tags are:

| Enum | Tag |
| --- | --- |
| `ArtifactKind::VerificationBundle` | `0x01` |
| `ArtifactKind::AcceptedSnapshot` | `0x02` |
| `ArtifactKind::ValidationReport` | `0x03` |
| `ArtifactKind::RejectedRows` | `0x04` |
| `ArtifactKind::DeduplicationReport` | `0x05` |
| `ArtifactSectionId::ValidationRuleSummary` | `0x01` |
| `ArtifactSectionId::ValidationFinding` | `0x02` |
| `ArtifactSectionId::RejectedRows` | `0x03` |
| `ArtifactSectionId::DedupRuleSummary` | `0x04` |
| `ArtifactSectionId::DuplicateFinding` | `0x05` |

An `optional(Uuid)` begins with `0x00` for `None` or `0x01` followed by the
16-byte UUID for `Some`. Every `repeated(...)` item is emitted exactly in the
order stated by its enclosing formula; counts are authoritative and a decoder
rejects trailing or missing bytes.

`canonical_schema_bytes` is the frozen byte encoding of a `LogicalSchema`
that the E4 runtime must expose as `LogicalSchema::canonical_bytes()` on
`stillflow-plan` (or an equivalent core-owned helper). The encoding is:

```text
u16(logical_schema_version)
|| u32(field_count)
|| repeated(
     u32(field_name_len) || utf8(field_name)
     || u8(nullable)
     || u8(logical_type_tag)
     || type_payload
     || u32(metadata_len) || repeated(u32(key_len) || utf8(key)
                                      || u32(value_len) || utf8(value))
   )
```

`logical_type_tag` and `type_payload` use the existing
`stillflow-core::LogicalType` serialization rules already frozen by the E2
contract; `metadata` is sorted by UTF-8 key bytes and must not contain secret
field names or values. The digest inputs never include display names that are
not part of the logical schema, allocator addresses, or filesystem paths.

`canonical_batch_bytes` is the Arrow 59 IPC record-batch message body for the
batch produced by the versioned E2 `BatchEnvelopeFactory`, with:
- little-endian Arrow IPC encoding;
- no compression;
- `IpcWriteOptions::default()`-equivalent metadata;
- the canonical schema message represented separately by
  `canonical_schema_bytes`, not repeated inside each batch digest;
- no transport headers, connection metadata, allocator addresses, or Parquet
  footer.
Batches are included in their logical sequence order.

`LogicalInputRef.version_digest` is defined as:

```text
SHA-256(logical-input-domain || u8(input_kind_tag) || u16(descriptor_version)
        || asset_id_bytes || canonical_schema_bytes)
```

where `input_kind_tag` is `0x01` for `InputRef::Asset` and `0x02` for the
reserved `InputRef::Snapshot`, `descriptor_version = 1` for this contract, and
`canonical_schema_bytes` is the authorized schema for the logical input (the
schema override when present, otherwise the connector-inspected schema). The
caller supplies `version_digest`; the engine recomputes it from the bound
asset/schema and rejects a mismatch.

`ArtifactPartition.digest` is
`SHA-256(partition-domain || artifact_id || section_id_tag ||
u32(sequence) || u64(row_count) || u64(stored_byte_count) ||
u32(canonical_batch_count) || repeated(u32(batch_len) || canonical_batch_bytes))`.
`canonical_batch_bytes` is the Arrow IPC record-batch message body defined
above. Batches are included in their logical sequence order. The runtime
records the resulting byte length as `stored_byte_count`; for this contract
that is the canonical logical payload byte count, not a filesystem allocation
or Parquet footer size. The physical Parquet representation remains immutable
storage, but its allocator, compression, and footer metadata are outside these
digests. The runtime does not hash a filesystem path or mutable file
metadata.

`ArtifactSection.section_digest` is
`SHA-256(section-domain || artifact_id || section_id_tag ||
canonical_schema_bytes || schema_fingerprint || u64(row_count) ||
u64(stored_byte_count) || u32(partition_count) ||
repeated(u32(sequence) || u64(row_count) || u64(stored_byte_count) ||
partition_digest))`, with partitions sorted by strictly increasing
`sequence`. `canonical_schema_bytes` is the frozen encoding defined above
(the E4 runtime exposes it as `LogicalSchema::canonical_bytes()`). The
section statistics must equal the sums of its partitions and
`partition_count` must equal the vector length.

`ArtifactManifest.manifest_digest` is
`SHA-256(manifest-domain || u16(version) || artifact_id || kind_tag ||
u32(section_count) || repeated(section_id_tag || canonical_schema_bytes ||
schema_fingerprint || u64(section.row_count) ||
u64(section.stored_byte_count) || u32(section.partition_count) ||
section_digest))`, with sections sorted by the fixed `ArtifactSectionId` tag order
`ValidationRuleSummary`, `ValidationFinding`, `RejectedRows`,
`DedupRuleSummary`, `DuplicateFinding`. The `manifest_digest` field itself is
excluded from this preimage. A manifest cannot contain duplicate section ids,
and its `sections` vector is stored in the canonical order.

For every report or rejected-rows artifact, committed
`ArtifactProvenance.content_digest` is
`SHA-256(artifact-provenance-domain || run_id || bundle_id || artifact_id ||
artifact_kind_tag || canonical_plan_digest || input.version_digest ||
u32(section_count) || repeated(section_id_tag || section_digest) ||
manifest_digest)`, using the same fixed section order. The provenance digest
does not include timestamps, `engine_build`, display names, or any secret or
filesystem metadata.

The accepted snapshot artifact does not acquire an `ArtifactManifest`.
Its manifest contribution is the explicit accepted-snapshot digest:

```text
accepted_snapshot_manifest_digest =
    SHA-256(accepted-snapshot-domain || snapshot_id || dataset_id
            || session_id || source_asset_id || schema_fingerprint
            || u64(row_count) || u64(stored_byte_count)
            || u32(partition_count)
            || repeated(u32(sequence) || u64(row_count)
                        || u64(stored_byte_count) || partition_digest))
```

where `partition_digest` is the same `ArtifactPartition.digest` formula used
for report artifacts, applied to each accepted `SnapshotPartition` in
strictly increasing sequence order. This digest is computed from the
committed `DatasetSnapshot` and `SnapshotPartition` values; it does not
require a new field on the existing `SnapshotManifest`. The E4 runtime may
additionally store this digest on `SnapshotManifest` for convenience, but the
formula above is authoritative.

The bundle-level provenance is not a child `ArtifactManifest`. Its
`content_digest` is
`SHA-256(bundle-provenance-domain || run_id || bundle_id ||
bundle_artifact_id || accepted_snapshot_id ||
validation_report_artifact_id || optional(rejected_rows_artifact_id) ||
deduplication_report_artifact_id || repeated(child_artifact_id ||
child_manifest_digest || child_content_digest))`. The child sequence is fixed
as accepted snapshot, validation report, optional rejected rows, then
deduplication report; the accepted snapshot contribution uses
`accepted_snapshot_manifest_digest` from the formula above. This separates
the transaction identity `bundle_id` from the bundle provenance artifact
identity.

`ArtifactSummary` is computed over the complete artifact, not independently
per section: `row_count`, `stored_byte_count`, and `partition_count` are the
respective sums across all sections; `finding_count` counts rows only in
`ValidationFinding` or `DuplicateFinding`; `warning_count` and `error_count`
count finding severities only in `ValidationFinding`; and `duplicate_count`
counts rows only in `DuplicateFinding`. A rule-summary row is not also counted
as a finding. The bundle-level summary is the sum of the committed summaries
of its present accepted and child artifacts, with each row counted once in its
own artifact.

`MAX_REPORT_ROWS`, `MAX_REPORT_BYTES`, and `MAX_REPORT_PARTITIONS` apply to each
report artifact after all of its sections are aggregated. The two always
present report artifacts additionally have the bundle-wide ceilings
`MAX_BUNDLE_REPORT_ROWS`, `MAX_BUNDLE_REPORT_BYTES`, and
`MAX_BUNDLE_REPORT_PARTITIONS`; rejected rows use the existing snapshot limits.
Exceeding either the per-artifact or bundle-wide ceiling fails with
`BoundExceeded` before commit.

`ArtifactSectionId` is one of `ValidationRuleSummary`,
`ValidationFinding`, `RejectedRows`, `DedupRuleSummary`,
`DuplicateFinding`.

Bundle membership is a storage row committed atomically with the accepted
snapshot and every artifact manifest:

```rust
pub struct VerificationBundleMembership {
    pub bundle_id: Uuid,
    pub run_id: Uuid,
    pub bundle_artifact_id: Uuid,
    pub accepted_snapshot_id: Uuid,
    pub validation_report_artifact_id: Uuid,
    pub rejected_rows_artifact_id: Option<Uuid>,
    pub deduplication_report_artifact_id: Uuid,
}
```

Readers open only through a bundle:

```rust
impl SnapshotStore {
    pub fn load_verification_bundle(&self, bundle_id: Uuid)
        -> Result<VerificationBundle, StorageError>;
    pub fn load_verification_bundle_by_snapshot(&self, snapshot_id: Uuid)
        -> Result<VerificationBundle, StorageError>;
    pub fn load_verification_bundle_by_run_id(&self, run_id: Uuid)
        -> Result<VerificationBundle, StorageError>;
    pub fn open_artifact_section(
        &self,
        bundle_id: Uuid,
        artifact_id: Uuid,
        section_id: ArtifactSectionId,
    ) -> Result<ArtifactBatchReader, StorageError>;
}
```

`ArtifactBatchReader` is a bounded iterator of `Result<BatchEnvelope,
StorageError>` with the same partition/row/byte limits as
`SnapshotBatchReader`. It cannot bypass bundle membership.

`VerificationBundleMembership` stores the exact `ArtifactManifest.artifact_id`
for each report/rejected artifact; the `_artifact_id` names are intentional and
do not refer to a separate manifest identity. `bundle_artifact_id` identifies
the bundle-level provenance record, while `bundle_id` identifies the atomic
visibility transaction. `rejected_rows_artifact_id` is `None` exactly when the
rejected artifact is absent.

`run_id` is unique among committed verification bundles. Therefore
`load_verification_bundle_by_run_id(run_id)` performs an exact membership lookup
and returns `StorageError::NotFound` when cancellation, failure, or an unknown
run has no committed bundle; it never scans or reconstructs an uncommitted
staging directory.

### 8.2 Artifacts

`materialize_verification` publishes exactly one `VerificationBundle`:

| Artifact | Presence | Sections |
| --- | --- | --- |
| Accepted snapshot | always | existing E2 `SnapshotManifest` rows |
| `ValidationReportArtifact` | always, zero rows allowed | `ValidationRuleSummary` + `ValidationFinding` |
| `RejectedRowsArtifact` | optional; present iff `terminal_rejection_count > 0` | `RejectedRows` |
| `DeduplicationReportArtifact` | always, zero rows allowed | `DedupRuleSummary` + `DuplicateFinding` |

The bundle plus its provenance is the only visibility boundary. No artifact
section may be loaded as visible without the bundle transaction that
contains all of its present members.

### 8.3 ValidationReportArtifact

`ValidationReportArtifact` has two sections.

`ValidationRuleSummary` section schema (one row per `Validate` rule):

```text
input_kind             : Utf8,   nullable = false
input_id               : Utf8,   nullable = false
input_version_digest   : Utf8,   nullable = false  // hex SHA-256
plan_fingerprint       : Utf8,   nullable = false  // FNV index only
canonical_plan_digest  : Utf8,   nullable = false  // SHA-256
node_id                : Utf8,   nullable = false
rule_ordinal           : UInt32, nullable = false
message                : Utf8,   nullable = false  // stored once per RuleRef
evaluated_count        : UInt64, nullable = false
pass_count             : UInt64, nullable = false
fail_count             : UInt64, nullable = false
warning_count          : UInt64, nullable = false
error_count            : UInt64, nullable = false
null_count             : UInt64, nullable = false
false_count            : UInt64, nullable = false
```

`ValidationFinding` section schema (one row per failing row; message is not
repeated):

```text
input_kind             : Utf8,   nullable = false
input_id               : Utf8,   nullable = false
input_version_digest   : Utf8,   nullable = false
source_row_ordinal     : UInt64, nullable = false
plan_fingerprint       : Utf8,   nullable = false
canonical_plan_digest  : Utf8,   nullable = false
node_id                : Utf8,   nullable = false
rule_ordinal           : UInt32, nullable = false
severity               : Utf8,   nullable = false  // "warning" | "error"
predicate_outcome      : Utf8,   nullable = false  // "false" | "null"
```

`ValidationFinding` is a row-level result, not a lifecycle event. Warning
findings never create a rejected-row payload. Error findings are terminal
and may create at most one rejected-row payload for the same source row.

### 8.4 RejectedRowsArtifact model

Rejected rows are terminal rows only. For each terminally rejected source
row the engine stores exactly one payload row in the `RejectedRows`
section:

```text
[logical Scan output schema fields, exact order]
+ input_kind             : Utf8,   nullable = false
+ input_id               : Utf8,   nullable = false
+ input_version_digest   : Utf8,   nullable = false
+ source_row_ordinal     : UInt64, nullable = false
+ rejection_kind         : Utf8,   nullable = false  // "validation_error" | "duplicate"
+ plan_fingerprint       : Utf8,   nullable = false
+ canonical_plan_digest  : Utf8,   nullable = false
+ node_id                : Utf8,   nullable = false
+ rule_ordinal           : UInt32, nullable = false
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

### 8.5 DeduplicationReportArtifact

`DeduplicationReportArtifact` has two sections.

`DedupRuleSummary` section schema (one row per `Deduplicate` rule):

```text
input_kind             : Utf8,   nullable = false
input_id               : Utf8,   nullable = false
input_version_digest   : Utf8,   nullable = false
plan_fingerprint       : Utf8,   nullable = false
canonical_plan_digest  : Utf8,   nullable = false
node_id                : Utf8,   nullable = false
rule_ordinal           : UInt32, nullable = false
key_column_count       : UInt32, nullable = false
evaluated_count        : UInt64, nullable = false
unique_count           : UInt64, nullable = false
duplicate_count        : UInt64, nullable = false
```

`DuplicateFinding` section schema (one row per duplicate row):

```text
input_kind                 : Utf8,   nullable = false
input_id                   : Utf8,   nullable = false
input_version_digest       : Utf8,   nullable = false
source_row_ordinal         : UInt64, nullable = false  // duplicate row
first_source_row_ordinal   : UInt64, nullable = false  // first-seen at this rule
plan_fingerprint           : Utf8,   nullable = false
canonical_plan_digest      : Utf8,   nullable = false
node_id                    : Utf8,   nullable = false
rule_ordinal               : UInt32, nullable = false
key_column_count           : UInt32, nullable = false
encoded_key_byte_count     : UInt32, nullable = false
```

The report contains no key bytes and no original cell values. It references
the **first-seen row at the Deduplicate rule**, not necessarily the
accepted row: the first-seen row may later be rejected by a Validate Error.
This wording remains frozen in R3.

### 8.6 Reserved control identities

The rejected rows control `ColumnId` values are fixed contract constants,
written in the later runtime crate and never generated at runtime:

```rust
pub const REJECTED_INPUT_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0011));
pub const REJECTED_INPUT_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0012));
pub const REJECTED_INPUT_VERSION_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0013));
pub const REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0014));
pub const REJECTED_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0015));
pub const REJECTED_PLAN_FINGERPRINT_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0016));
pub const REJECTED_CANONICAL_PLAN_DIGEST_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0017));
pub const REJECTED_NODE_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0018));
pub const REJECTED_RULE_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0019));
```

Source schemas must not already contain these reserved names or ids; a
collision is preflight `InvalidPlan`. The rejected artifact schema has
`source_field_count + 9 <= MAX_SCHEMA_FIELDS`; a source schema with more
than `MAX_SCHEMA_FIELDS - 9` fields is preflight `InvalidPlan`.

### 8.7 Report section ColumnId constants

Every report section field has a fixed `ColumnId`. The runtime never
generates a report `ColumnId`. The values use the same reserved `0xE4C0`
namespace as the rejected-row control columns.

`ValidationRuleSummary` fields:

| Field | ColumnId |
| --- | --- |
| `input_kind` | `0x...0021` |
| `input_id` | `0x...0022` |
| `input_version_digest` | `0x...0023` |
| `plan_fingerprint` | `0x...0024` |
| `canonical_plan_digest` | `0x...0025` |
| `node_id` | `0x...0026` |
| `rule_ordinal` | `0x...0027` |
| `message` | `0x...0028` |
| `evaluated_count` | `0x...0029` |
| `pass_count` | `0x...002A` |
| `fail_count` | `0x...002B` |
| `warning_count` | `0x...002C` |
| `error_count` | `0x...002D` |
| `null_count` | `0x...002E` |
| `false_count` | `0x...002F` |

`ValidationFinding` fields:

| Field | ColumnId |
| --- | --- |
| `input_kind` | `0x...0031` |
| `input_id` | `0x...0032` |
| `input_version_digest` | `0x...0033` |
| `source_row_ordinal` | `0x...0034` |
| `plan_fingerprint` | `0x...0035` |
| `canonical_plan_digest` | `0x...0036` |
| `node_id` | `0x...0037` |
| `rule_ordinal` | `0x...0038` |
| `severity` | `0x...0039` |
| `predicate_outcome` | `0x...003A` |

`DedupRuleSummary` fields:

| Field | ColumnId |
| --- | --- |
| `input_kind` | `0x...0041` |
| `input_id` | `0x...0042` |
| `input_version_digest` | `0x...0043` |
| `plan_fingerprint` | `0x...0044` |
| `canonical_plan_digest` | `0x...0045` |
| `node_id` | `0x...0046` |
| `rule_ordinal` | `0x...0047` |
| `key_column_count` | `0x...0048` |
| `evaluated_count` | `0x...0049` |
| `unique_count` | `0x...004A` |
| `duplicate_count` | `0x...004B` |

`DuplicateFinding` fields:

| Field | ColumnId |
| --- | --- |
| `input_kind` | `0x...0051` |
| `input_id` | `0x...0052` |
| `input_version_digest` | `0x...0053` |
| `source_row_ordinal` | `0x...0054` |
| `first_source_row_ordinal` | `0x...0055` |
| `plan_fingerprint` | `0x...0056` |
| `canonical_plan_digest` | `0x...0057` |
| `node_id` | `0x...0058` |
| `rule_ordinal` | `0x...0059` |
| `key_column_count` | `0x...005A` |
| `encoded_key_byte_count` | `0x...005B` |

The shorthand `0x...00XX` means the full UUID
`0xE4C0_0000_0000_4000_8000_0000_0000_00XX`. The same collision rule as
the rejected-row controls applies: source/report schemas must not already
contain these ids, and a collision is preflight `InvalidPlan`.

### 8.8 Original-value preservation decision

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

1. Paths are `dedup_{run_id}.sqlite` and `dedup_{run_id}.lock` under the
   storage-managed temp root. `run_id` and `bundle_id` are caller-injected
   and non-nil.
2. The storage crate creates the temp directory with permissions `0700`
   (Unix). It then exclusively creates the `.lock` path with
   `create_new(true)`, records that this attempt created it, and acquires the
   exclusive OS lock before touching the `.sqlite` path.
   While holding the new `.lock`, it exclusively creates the `.sqlite` path
   with `create_new(true)`, records that this attempt created it, and opens
   SQLite. If either path existed before this attempt, opening fails with
   `StorageError::AlreadyExists`. The failure handler may delete only paths
   recorded as created by this attempt; it never deletes a pre-existing
   `.sqlite` or `.lock`.
3. The handle keeps the exclusive OS file lock on
   `dedup_{run_id}.lock` for the lifetime of the index. This lock, not only
   `started_at`, is the active-ownership signal.
4. 4. Immediately after SQLite opens and before creating any table or lease
   row, the handle sets `PRAGMA page_size = 4096`,
   `PRAGMA max_page_count = MAX_DEDUP_INDEX_PAGES`,
   `PRAGMA cache_size = -512`, and `PRAGMA journal_mode = DELETE`.
   Because the `.sqlite` file was exclusively created by this attempt,
   `page_size` applies to the new database before its first table is
   created.
5. After the PRAGMAs are applied, the handle writes an ownership lease row
   containing `run_id`, `bundle_id`, and `started_at`. The lease is
   advisory recovery metadata; the file lock and `create_new` are the
   primary ownership guards.
6. File permission is `0600` (Unix). On platforms without Unix modes, the
   storage crate applies the strongest equivalent owner-only ACL available
   and records that behavior in its tests.

`PRAGMA cache_size = -512` is a soft page-cache target (512 KiB), not a
strict SQLite memory cap. The contract does not claim it enforces a hard
512 KiB total memory limit. Strict resource enforcement is provided by
`PRAGMA max_page_count`, reserve-before-allocate, the application-level
dedup page/byte caps, and the engine/storage peak laws. The runtime must
document and test the actual memory behavior rather than treating
`cache_size` as a hard ceiling.

The creation protocol has explicit crash points after lock-file creation,
after lock acquisition, after SQLite-file creation, after SQLite open, and
after lease initialization. A crash at any point is recoverable because the
next recovery scans both filename suffixes. A failed second-file creation
rolls back the first file only when the first file was created by this attempt;
rollback failure is reported and leaves a recoverable candidate.

### 9.2 Dedup table

```sql
CREATE TABLE dedup_index (
    node_id                   BLOB    NOT NULL,
    rule_ordinal              INTEGER NOT NULL,
    key_bytes                 BLOB    NOT NULL,
    first_source_row_ordinal  INTEGER NOT NULL,
    PRIMARY KEY (node_id, rule_ordinal, key_bytes)
) WITHOUT ROWID;

CREATE TABLE dedup_lease (
    run_id          BLOB NOT NULL PRIMARY KEY,
    bundle_id       BLOB NOT NULL,
    started_at_utc  TEXT NOT NULL
) WITHOUT ROWID;
```

- `node_id` is the exact 16-byte `Uuid` of `PlanNodeId`.
- `rule_ordinal` is zero-based within the containing `ApplyRules` node.
- `first_source_row_ordinal` is the logical Scan output ordinal of the
  first row inserted for that `(node_id, rule_ordinal, key_bytes)`.
- Insert API returns a typed result:

```rust
pub enum DedupInsert {
    Inserted { first_source_row_ordinal: u64 },
    Duplicate { first_source_row_ordinal: u64 },
}

impl DedupIndex {
    pub fn insert_first(
        &self,
        node_id: Uuid,
        rule_ordinal: u32,
        key_bytes: &[u8],
        current_source_row_ordinal: u64,
    ) -> Result<DedupInsert, StorageError>;
}
```

- Insert decision: `INSERT INTO dedup_index (...) VALUES (...)
  ON CONFLICT DO NOTHING`; `changes() == 1` means first occurrence and the
  returned ordinal is `current_source_row_ordinal`; `0` means duplicate and
  the returned ordinal is read from the existing row. The engine never
  computes a hash to decide.
- SQLite BLOB primary-key equality over full canonical bytes is the only
  equality path. No hash-only or approximate path exists.

### 9.3 Close, recovery, and maintenance gate

- `DedupIndex::close_and_delete(self) -> Result<(), StorageError>` is the
  explicit contract. It releases the `.lock`, closes SQLite, deletes both
  `.sqlite` and `.lock`, and returns an error if deletion fails. The engine
  calls it after the last dedup insert and **before**
  `VerificationBundleWriter::commit`. If deletion fails, the bundle is
  aborted and no artifact becomes visible.
- `Drop` is defense-in-depth only: it attempts best-effort deletion but its
  errors are not a substitute for `close_and_delete`.
- No open path ever deletes a pre-existing file. Stale files are removed
  only by storage recovery, and recovery runs under the existing storage
  **maintenance gate** and root file lock.
- Recovery scans the union of `dedup_*.sqlite` and `dedup_*.lock`, derives the
  `run_id` candidate from either filename, and evaluates each candidate as a
  pair. If a `.lock` exists, recovery first tries to acquire it. If
  acquisition fails, the run is active and neither file is removed. If
  acquisition succeeds, or if only an orphan `.sqlite` exists with no
  `.lock`, the candidate is stale/orphaned and both paths are removed if
  present. A lone `.lock` is handled the same way after the lock is acquired.
- A process crash releases its OS lock but may leave either or both files. The
  crashed run cannot have committed its bundle because index cleanup precedes
  commit; recovery removes the complete pair or orphan and records the
  cleanup. Retry with the same `run_id` before recovery returns
  `AlreadyExists`; after recovery it starts from an empty index.
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
| Active ownership | exclusive OS lock on `dedup_{run_id}.lock`; never deleted while held |

## 10. Atomic publication and security

### 10.1 VerificationBundle transaction

Publication sequence for `materialize_verification`:

1. Apply the default deadline and acquire the existing E2 run-gate permit
   (`try_acquire`, never await).
2. Run shared E2 preflight with E4 target disabled and the E4 rule checks.
3. Validate injected identities, input ref, provenance fields, and pairwise
   uniqueness of `bundle_artifact_id`, accepted snapshot id, and every
   present child artifact id independently from `bundle_id`.
4. `SnapshotStore::begin_verification_bundle(draft, started_at)`. This
   acquires exactly one storage publisher permit and creates one bundle
   staging context for all present and potential artifacts.
5. `SnapshotStore::open_dedup_index(run_id, bundle_id, started_at)`.
6. Open the connector stream exactly once and process accepted rows,
   validation findings, rejected payloads, and duplicate findings.
7. Call `DedupIndex::close_and_delete()`.
8. `VerificationBundleWriter::commit(committed_at)`, which makes accepted
   snapshot, validation report, optional rejected artifact, deduplication
   report, and bundle provenance visible in one SQLite transaction.
9. Return `VerificationBundle`.

The commit is the only visibility point. A reader either loads the complete
bundle by `bundle_id`, by `run_id`, or by accepted snapshot id, or sees none
of it. There is no API that loads validation report, rejected rows, or
deduplication report independently of the bundle transaction.

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
bundle, and leaves no partial partition, report file, or temp index after
normal cleanup. If cleanup itself is interrupted or fails, the run still
publishes no bundle and the resulting `.sqlite` / `.lock` candidate is
recoverable by the maintenance-gated recovery protocol in section 9.3.
A storage error during bundle commit rolls back every manifest row and every
installed partition directory. Tests must assert that both
`load_verification_bundle_by_run_id(run_id)` and
`load_verification_bundle(bundle_id)` fail after every failure injection and
that no child artifact is independently visible.

Dropping an uncommitted `VerificationBundleWriter` aborts the whole bundle
staging context. `DedupIndex::Drop` remains best-effort and is followed by
recovery as needed.

### 10.4 Crash and recovery state machine

The bundle writer moves through explicit storage states. Recovery is owned
by `SnapshotStore` and uses the existing maintenance gate and root file lock.
This state machine is bound to the verified storage facts from PR #62
(`docs/issues/storage-publication-recovery-inventory.md`): publication journal
commit precedes staging creation, final files precede SQLite visibility, and
visibility plus journal deletion share one SQLite transaction. The contract
does not claim process-kill or power-loss durability beyond what that inventory
records as untested.

| State | Meaning | Crash recovery |
| --- | --- | --- |
| `Prepared` | Draft validated; publication journal row committed; no staging directory yet | recovery removes the committed publication row (abort publication) and any temp files created by this attempt; no manifest row exists |
| `Staged` | Accepted/report/rejected partitions written under the bundle staging directory | recovery removes stale bundle staging directory and the committed publication row; no SQLite manifest row exists |
| `Installing` | Final artifact directories created but SQLite commit has not begun | recovery removes installed artifact directories and staging; no manifest row exists; bundle is not visible |
| `Committing` | SQLite transaction is installing bundle membership and all manifests | the SQLite transaction is atomic; a crash before commit leaves the previous state, a crash after commit leaves the complete bundle visible |
| `Committed` | bundle is visible and complete | no cleanup; bundle can be read by `bundle_id`, `run_id`, or accepted snapshot id |

Tests must cover each state with injected process-crash/storage-failure
fixtures and assert that no partial bundle is ever visible.

### 10.5 Identity and timestamp injection

| Field | Source |
| --- | --- |
| `run_id` | `identities.run_id` |
| `bundle_id` | `identities.bundle_id` |
| `logical_input` | `identities.logical_input`, including `version_digest` |
| `canonical_plan_digest` | `identities.canonical_plan_digest` is a caller-supplied expected value; the engine recomputes SHA-256 over `LogicalPlan::canonical_bytes()`, rejects a mismatch, and inserts the recomputed value into every provenance draft |
| Accepted snapshot id / dataset id | `identities.snapshot_id` / `identities.dataset_id` |
| Bundle provenance artifact id | `identities.bundle_artifact_id` |
| Validation report artifact id | `identities.validation_report_artifact_id` |
| Rejected rows artifact id | `identities.rejected_rows_artifact_id` (used only if present) |
| Deduplication report artifact id | `identities.deduplication_report_artifact_id` |
| `session_id` | `identities.session_id` for accepted snapshot and bundle provenance |
| `source_asset_id` | bound `SourceAsset.id` |
| `input_ref` | `InputRef::Asset { asset_id: bound SourceAsset.id }` |
| `lineage` | `identities.lineage` |
| Accepted `quality_score` | `identities.quality_score` |
| `created_at` / `started_at` / `committed_at` | `identities.created_at` / `identities.started_at` / `identities.committed_at` |
| Dedup temp file identity | `identities.run_id`, `identities.bundle_id`, and `identities.started_at` |

The engine must not call `Uuid::new_v4` or `Utc::now` for any published
identity or timestamp. Wall-clock `Instant` remains allowed only for
deadline observation. Nil ids, nil lineage ids, quality scores above 100,
duplicate artifact ids, and timestamp order violations are rejected before
`begin_verification_bundle`.

`bundle_id` is the atomic visibility identity, not an artifact identity.
`bundle_artifact_id`, `snapshot_id`, `validation_report_artifact_id`,
`deduplication_report_artifact_id`, and any present
`rejected_rows_artifact_id` must all be non-nil and pairwise distinct from
`run_id` and from one another. When `terminal_rejection_count == 0`,
`rejected_rows_artifact_id` must be `None` and no rejected provenance or
manifest is constructed.

### 10.6 Security boundary for raw values

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

### 10.7 Validation message safety

- The plan-authored validation message is report data, not a log. It is
  stored once per `RuleRef` in the `ValidationRuleSummary` section, never
  repeated per finding, and never in `EngineError` or logs.
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
- `run_id`, `bundle_id`, artifact ids, `plan_fingerprint`,
  `canonical_plan_digest`, `node_id`, `rule_ordinal`,
  severity, row ordinal, batch sequence, and resource counts are safe
  correlation metadata and may appear in sanitized errors.

### 10.8 Error surface

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
pub const MAX_REPORT_PARTITIONS: u32 = MAX_SNAPSHOT_PARTITIONS; // 16,384
pub const REPORT_PACK_ROWS: usize = 1_024;
pub const REPORT_PACK_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_REPORT_ROWS: u64 =
    (MAX_REPORT_PARTITIONS as u64) * (REPORT_PACK_ROWS as u64); // 16,777,216
pub const MAX_REPORT_BYTES: u64 =
    (MAX_REPORT_PARTITIONS as u64) * (REPORT_PACK_BYTES as u64); // 32 GiB
pub const MAX_BUNDLE_REPORT_PARTITIONS: u32 = 2 * MAX_REPORT_PARTITIONS;
pub const MAX_BUNDLE_REPORT_ROWS: u64 = 2 * MAX_REPORT_ROWS;
pub const MAX_BUNDLE_REPORT_BYTES: u64 = 2 * MAX_REPORT_BYTES;
pub const MAX_VALIDATION_FINDINGS_PER_RUN: u64 = MAX_REPORT_ROWS;
pub const MAX_DUPLICATE_FINDINGS_PER_RUN: u64 = MAX_REPORT_ROWS;
pub const MAX_VALIDATION_MESSAGE_BYTES: usize = 1_024;
pub const MAX_REPORT_REMAINDER_BYTES: usize = REPORT_PACK_BYTES;
pub const VERIFICATION_MAX_COMPILED_PLAN_BYTES: usize = 3 * 1024 * 1024;
pub const VERIFICATION_MAX_ROUTING_STATE_BYTES: usize = 512 * 1024;

pub struct VerificationIdentities {
    pub run_id: Uuid,
    pub bundle_id: Uuid,
    pub bundle_artifact_id: Uuid,
    pub snapshot_id: Uuid,
    pub dataset_id: Uuid,
    pub validation_report_artifact_id: Uuid,
    pub rejected_rows_artifact_id: Option<Uuid>,
    pub deduplication_report_artifact_id: Uuid,
    pub session_id: Uuid,
    pub logical_input: LogicalInputRef,
    pub canonical_plan_digest: [u8; 32],
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
    pub provenance: ArtifactProvenanceDraft,
    pub accepted: SnapshotDraft,
    pub validation_report_artifact_id: Uuid,
    pub rejected_rows_artifact_id: Option<Uuid>,
    pub deduplication_report_artifact_id: Uuid,
}

pub struct VerificationBundle {
    pub membership: VerificationBundleMembership,
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

pub enum ArtifactSectionId {
    ValidationRuleSummary,
    ValidationFinding,
    RejectedRows,
    DedupRuleSummary,
    DuplicateFinding,
}

pub struct ArtifactManifest {
    pub version: u16,
    pub artifact_id: Uuid,
    pub kind: ArtifactKind,
    pub sections: Vec<ArtifactSection>,
    pub manifest_digest: ContentDigest,
}

pub struct ArtifactSection {
    pub section_id: ArtifactSectionId,
    pub schema: LogicalSchema,
    pub schema_fingerprint: LogicalSchemaFingerprint,
    pub stats: ArtifactSectionStats,
    pub partitions: Vec<ArtifactPartition>,
    pub section_digest: ContentDigest,
}

pub struct ArtifactSectionStats {
    pub row_count: u64,
    pub stored_byte_count: u64,
    pub partition_count: u32,
}

pub struct ArtifactPartition {
    pub sequence: u32,
    pub row_count: u64,
    pub stored_byte_count: u64,
    pub digest: ContentDigest,
}

pub struct VerificationBundleMembership {
    pub bundle_id: Uuid,
    pub run_id: Uuid,
    pub bundle_artifact_id: Uuid,
    pub accepted_snapshot_id: Uuid,
    pub validation_report_artifact_id: Uuid,
    pub rejected_rows_artifact_id: Option<Uuid>,
    pub deduplication_report_artifact_id: Uuid,
}

pub struct ArtifactBatchReader {
    // Iterator over Result<BatchEnvelope, StorageError>; bounded by storage limits.
}

pub enum DedupInsert {
    Inserted { first_source_row_ordinal: u64 },
    Duplicate { first_source_row_ordinal: u64 },
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
        bundle_id: Uuid,
    ) -> Result<VerificationBundle, StorageError>;

    pub fn load_verification_bundle_by_snapshot(
        &self,
        snapshot_id: Uuid,
    ) -> Result<VerificationBundle, StorageError>;

    pub fn load_verification_bundle_by_run_id(
        &self,
        run_id: Uuid,
    ) -> Result<VerificationBundle, StorageError>;

    pub fn open_artifact_section(
        &self,
        bundle_id: Uuid,
        artifact_id: Uuid,
        section_id: ArtifactSectionId,
    ) -> Result<ArtifactBatchReader, StorageError>;

    pub fn open_dedup_index(
        &self,
        run_id: Uuid,
        bundle_id: Uuid,
        started_at: DateTime<Utc>,
    ) -> Result<DedupIndex, StorageError>;
}

impl VerificationBundleWriter {
    pub fn append_accepted(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn append_validation_rule_summary(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn append_validation_findings(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn append_rejected_rows(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn append_dedup_rule_summary(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn append_duplicate_findings(&mut self, envelope: &BatchEnvelope) -> Result<(), StorageError>;
    pub fn commit(self, committed_at: DateTime<Utc>) -> Result<VerificationBundle, StorageError>;
}

impl DedupIndex {
    pub fn insert_first(
        &self,
        node_id: Uuid,
        rule_ordinal: u32,
        key_bytes: &[u8],
        current_source_row_ordinal: u64,
    ) -> Result<DedupInsert, StorageError>;

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

Reserve-before-allocate law: every accepted/rejected/report remainder
builder computes the exact candidate bytes **before** appending, calls
`try_reserve` for that exact peak, and only then copies/moves data. A failed
reserve or a predicted over-cap is `EngineError::BoundExceeded` before any
visible write. Allocator tests track a `Report` phase separately from the
E2 `Connector`/`Polars`/`Remainder`/`StorageAppend` phases. Parquet encode
and `SnapshotWriter::append` scratch inside `stillflow-storage` remain
storage-phase memory and are excluded from the 265 MiB engine peak, matching
E2 §14.1.

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

E4-C0-R3 does **not** extend `PreviewResult`, `PreviewRequest`, or the E3
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
| Report max partitions | `MAX_SNAPSHOT_PARTITIONS` = 16,384 | this contract |
| Report max rows | `MAX_REPORT_ROWS` = 16,777,216 | `MAX_REPORT_PARTITIONS * REPORT_PACK_ROWS` |
| Report max bytes | `MAX_REPORT_BYTES` = 32 GiB | `MAX_REPORT_PARTITIONS * REPORT_PACK_BYTES` |
| Bundle report max partitions | `MAX_BUNDLE_REPORT_PARTITIONS` = 32,768 | two report artifacts after section aggregation |
| Bundle report max rows | `MAX_BUNDLE_REPORT_ROWS` = 33,554,432 | `2 * MAX_REPORT_ROWS` |
| Bundle report max bytes | `MAX_BUNDLE_REPORT_BYTES` = 64 GiB | `2 * MAX_REPORT_BYTES` |
| Report remainder bytes | 2 MiB each | this contract |
| Dedup key columns per rule | 64 | this contract |
| Encoded composite dedup key | 64 KiB | this contract |
| Dedup index disk | 8 GiB | `PRAGMA max_page_count` = 2,097,152 |
| Dedup index rows per run | `MAX_SNAPSHOT_ROWS` (1,000,000,000) | storage |
| Validation findings per source row | 256 | this contract |
| Validation findings per run | `MAX_REPORT_ROWS` = 16,777,216 | this contract |
| Duplicate findings per run | `MAX_REPORT_ROWS` = 16,777,216 | this contract |
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
| Storage append/Parquet encode scratch | not counted in engine peak; counted in storage phase | E2 §14.1 |
| Default / maximum deadline | 15 min / 30 min | E2 |

Exceeding any ceiling is a typed error before visible publication. No
placeholder value is permitted.

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
  `run_id` still has an active `.lock` or unexpired lease, the retry fails with
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
| V03 | Cross-batch global dedup first-seen | Distinct keys span at least three execution chunks/envelopes; only the lowest `source_row_ordinal` per key is first-seen at the rule; each later row produces one rejected payload and one duplicate finding. |
| V04 | Connector partition invariance | Two partitionings of the same ordered rows produce identical accepted rows, validation findings, rejected rows, duplicate findings, schemas, summaries, and envelope boundaries. |
| V05 | Null, NaN, `-0.0`/`+0.0` key equality | Null duplicates null; all NaN bit patterns group together; `-0.0` and `+0.0` are duplicates; finite distinct floats remain distinct. |
| V06 | Multiple Validate hits and one-payload guarantee | Warning then Error on one row emits both findings in rule order; first Error terminates later rules; rejected artifact contains exactly one payload for that row; 256-finding cap fails with `BoundExceeded` and no bundle. |
| V07 | Warning rows never enter rejected artifact | Warning-only fixture has zero terminal rejections, `rejected_rows = None`, and warning finding references the accepted row. |
| V08 | Duplicate rows enter rejected artifact and dedup report, not silent deletion | Each duplicate has one payload with `rejection_kind = "duplicate"`, one `DuplicateFinding` with first-seen `SourceRowRef`, and no key bytes in the report. |
| V09 | Cancellation and deadline publish nothing | Inject cancel/deadline at each section 10.3 point; both `load_verification_bundle_by_run_id(run_id)` and `load_verification_bundle(bundle_id)` fail; normal cleanup leaves no temp index, while an interrupted cleanup leaves only a recoverable `.sqlite`/`.lock` candidate; no staging residue. |
| V10 | Bundle atomicity | Inject failure during commit after accepted partition install; neither accepted snapshot nor any report/rejected artifact is independently visible; rollback cleans all files. |
| V11 | Zero-rejection rule | No rejected artifact row is inserted when terminal rejection count is zero; storage creates no empty DatasetSnapshot for rejected rows. |
| V12 | Dedup index ownership and recovery | Failure-inject lock-first creation after each creation step; a pre-existing `.sqlite` or `.lock` returns `AlreadyExists` and is never deleted; a newly created first file is rolled back on second-file failure; recovery scans the union of both suffixes, removes orphan/pairs only under the maintenance gate after acquiring the lock when present, and never removes an active locked pair. |
| V13 | Dedup index permissions and page cap | Temp dir mode `0700`, DB file mode `0600`; `PRAGMA max_page_count` equals `MAX_DEDUP_INDEX_PAGES`; disk > 8 GiB fails `BoundExceeded`. |
| V14 | Memory ceiling | Instrumented live-payload counter shows `<= 6` and no seventh payload; allocator/SQLite cache stay within section 12.2; source grep and allocator prove no in-memory `HashSet`/`HashMap` dedup index. |
| V15 | Secret sentinel | Sentinel appears in a failing cell but not in `EngineError` Display/Debug, `sanitized_summary()` JSON, event metadata, reports, or provenance summaries. |
| V16 | Retry determinism | Retry after recovery/fresh run id with identical inputs/identities produces identical artifacts and partition boundaries; pre-existing active dedup file is never deleted. |
| V17 | Utf8 and Binary key equality | Exact byte equality; empty string/binary distinct from null; no normalization/collation. |
| V18 | Timestamp key boundary and equality | Millisecond/Microsecond/Nanosecond same-type same-epoch duplicates; `Timestamp { unit: Second }`, `List`, and `Struct` keys are preflight `TypeError`. |
| V19 | Canonical key bytes and collision safety | Golden vectors for every supported tag, including `timezone: None` vs `Some` presence encoding; different values never produce equal bytes; SQLite BLOB PK is the only duplicate decision path. |
| V20 | Key bounds | The 65th key column fails preflight; fixed-width key maxima are checked in preflight; every actual fixed-width, Utf8, and Binary key is encoded and checked immediately before SQLite insert, so an encoded key > 64 KiB fails `BoundExceeded` before any index write; the SQLite `max_page_count` cap also fails before bundle commit with no visible artifact. |
| V21 | Schema/ColumnId/original value preservation | Rejected schema field order and metadata match logical Scan output + nine control fields; ColumnIds unchanged; Arrow values equal source values including null/NaN/`-0.0`; at most one payload per source row. |
| V22 | Validation message safety and length | Explicit E4 preflight rejects empty-after-trim, > 1,024 bytes, and secret-like message; exact safe message stored once per `RuleRef` in the validation rule summary; absent from errors/logs/events. |
| V23 | Existing E2/E3 compatibility | `materialize` still returns `UnsupportedRule` for Validate/Deduplicate; `preview` behavior is unchanged by the E4 code path; no `PreviewResult` field changed. |
| V24 | Provenance completeness and CI | Every artifact embeds committed `ArtifactProvenance`; callers provide only `ArtifactProvenanceInput`, while the engine supplies the verified canonical-plan SHA-256, contract versions, and compile-time `engine_build`; the writer supplies summary/content digest. The result includes `run_id`, `bundle_id`, distinct `artifact_id`, `session_id`, `LogicalInputRef`, FNV index, lineage, and injected times. CI checks pass in the later runtime PR; this docs PR modifies only the file named in section 2. |

| V25 | Storage round-trip through bundle reader | Write a bundle containing accepted, validation summary/findings, rejected rows, and dedup summary/findings; load by `bundle_id`, by `run_id`, and by accepted snapshot id; open each `ArtifactSection` through `ArtifactBatchReader` and assert row/partition/manifest/section/provenance digest equality. |
| V26 | Provenance draft vs committed | `VerificationBundleDraft` contains engine-assembled provenance with no summary/content_digest; after commit, every artifact provenance contains summary and content digest; `bundle_id` is distinct from `run_id`, `bundle_artifact_id`, and every child artifact id; `session_id` is present. |
| V27 | Rule summaries and message once | `ValidationRuleSummary` contains evaluated/pass/fail/warning/error/null/false counts and message once per `RuleRef`; findings contain no message; `DedupRuleSummary` contains evaluated/unique/duplicate counts. |
| V28 | Dedup insert typed first ordinal | `insert_first()` returns `Inserted` or `Duplicate` with `first_source_row_ordinal`; duplicate finding uses that ordinal even if the first-seen row is later rejected by Validate Error. |
| V29 | Report resource math | Exactly `MAX_REPORT_ROWS = MAX_REPORT_PARTITIONS * REPORT_PACK_ROWS` and `MAX_REPORT_BYTES = MAX_REPORT_PARTITIONS * REPORT_PACK_BYTES`; limits are enforced after aggregating all sections of each report artifact and again at the two-report bundle ceiling; exceeding row/byte/partition limits fails `BoundExceeded`; no writer can emit a partition count above the applicable ceiling. |
| V30 | Crash and maintenance recovery | Inject process crash at bundle states `Prepared` (including the journal-commit-before-staging window), `Staged`, `Installing`, and `Committing`, and at every dedup creation point in section 9.1; no partial bundle is visible; recovery under the maintenance gate removes the committed publication row, stale bundle staging, and every stale/orphan dedup suffix pair only after acquiring the lock when present; active bundle/index is untouched. |

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
- The reserved nine rejected-control fields reduce the maximum cleanable
  source schema from 4,096 to 4,087 fields. This is frozen, not dynamic.
- Timestamp equality is type-local and E4-C0 excludes Second/List/Struct.
  Any future expansion must not reuse the reserved tags without a new
  approved contract.
- PR #53 may still revise the E3 public surface. E4 runtime must not start
  until PR #53 merges and must reconcile only section 13 against the merged
  API.
