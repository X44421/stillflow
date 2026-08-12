# Issue #7 implementation contract: bounded workbook analysis

Status: frozen for implementation  
Risk: `risk:high`  
Accepted base: `main@ebd913bce09318277eee27c4aca318a3aacfada9`  
Frozen: 2026-08-12

## 1. Objective

Implement a read-only Excel/OpenDocument connector that discovers worksheets,
returns deterministic header and data-region candidates, and exposes explicitly
selected regions as canonical Arrow 59 `BatchEnvelope` streams.

The implementation must support XLS, XLSX, XLSM, XLSB and ODS without assuming
that the first row is a header, executing formulas, following filesystem links,
or turning preview into an unbounded import.

## 2. Accepted dependency and package boundary

- Add `stillflow-connector-workbook` as an adapter crate.
- The adapter depends on `stillflow-connectors` and `stillflow-core`; neither
  shared crate depends on the adapter.
- Pin Calamine to `=0.35.0` with its `chrono` feature. Calamine 0.35 has an
  upstream MSRV of Rust 1.83 and contains the January 2026 ODS repeat-expansion
  hardening. Calamine 0.36.1 requires Rust 1.88 and is therefore outside the
  workspace Rust 1.85 contract.
- Use Arrow 59 builders directly. Do not introduce Polars, DuckDB, SQLx, Axum,
  `object_store`, or the `arrow` meta-crate into this adapter.
- Reuse `BatchEnvelopeFactory`; one validated factory and canonical `SchemaRef`
  are retained for the lifetime of each prepared region reader.
- Filesystem traversal uses capability directory handles and component-by-
  component no-follow opens, matching the local-tabular security boundary.
- The committed lockfile is part of the change and must pass Rust 1.85 and
  current stable CI.

References:

- https://github.com/tafia/calamine/tree/v0.35.0
- https://docs.rs/calamine/0.35.0/calamine/
- https://github.com/tafia/calamine/pull/596

## 3. Public domain additions

### 3.1 Coordinate model

Add serializable workbook types to `stillflow-core`:

```rust
pub struct CellCoordinate {
    pub row: u32,
    pub column: u32,
}

pub struct CellRange {
    pub start: CellCoordinate,
    pub end: CellCoordinate,
}

pub enum WorkbookHeaderSelection {
    NoHeader,
    Row(u32),
}

pub struct WorkbookRegionSelection {
    pub range: CellRange,
    pub header: WorkbookHeaderSelection,
}
```

Rows and columns are zero-based and both range endpoints are inclusive. Public
validation rejects inverted ranges and arithmetic overflow.

`AssetLocator` gains an optional `workbook_region` field. A discovered sheet has
`sheet = Some(name)` and `workbook_region = None`. Preview and read require an
explicit selection copied from inspection output. `NoHeader` is distinct from
an omitted selection.

### 3.2 Inspection model

Add:

```rust
pub enum CandidateConfidence { Low, Medium, High }

pub struct WorkbookHeaderCandidate {
    pub row: u32,
    pub confidence: CandidateConfidence,
    pub score: u8,
}

pub struct WorkbookRegionCandidate {
    pub range: CellRange,
    pub confidence: CandidateConfidence,
    pub non_empty_cells: u64,
    pub header_candidates: Vec<WorkbookHeaderCandidate>,
}

pub struct WorkbookInspection {
    pub sheet_visibility: WorkbookSheetVisibility,
    pub formula_cells: u64,
    pub merged_regions: Vec<CellRange>,
    pub hidden_rows: Vec<u32>,
    pub hidden_columns: Vec<u32>,
    pub region_candidates: Vec<WorkbookRegionCandidate>,
    pub analysis_truncated: bool,
}
```

`AssetMetadata` gains `workbook: Option<WorkbookInspection>` with serde defaults
so existing serialized metadata remains readable. Non-workbook connectors set
it to `None`.

## 4. Connection configuration

The connector requires `ConnectorKind::ExcelWorkbook` and accepts:

```json
{
  "allowedRoots": ["/absolute/root"],
  "maxDiscoveryDepth": 16,
  "maxDiscoveredAssets": 10000,
  "maxWorkbookBytes": 67108864,
  "maxArchiveEntries": 4096,
  "maxExpandedArchiveBytes": 268435456,
  "maxSheetCells": 2000000,
  "maxRegionCandidates": 128,
  "analysisRows": 10000,
  "analysisColumns": 256
}
```

Bounds:

| Setting | Default | Accepted maximum |
| --- | ---: | ---: |
| discovery depth | 16 | 64 |
| discovered sheet assets | 10,000 | 100,000 |
| compressed workbook bytes | 64 MiB | 256 MiB |
| archive entries | 4,096 | 16,384 |
| expanded archive bytes | 256 MiB | 1 GiB |
| decoded cells per selected sheet | 2,000,000 | 4,000,000 |
| region candidates | 128 | 1,024 |
| analyzed rows | 10,000 | 100,000 |
| analyzed columns | 256 | 4,096 |

Unknown fields, empty roots, relative roots, zero bounds and values above the
accepted maxima are invalid configuration. Configuration never contains raw
credentials.

## 5. Discovery and identity

- Supported suffixes are `.xls`, `.xlsx`, `.xlsm`, `.xlsb` and `.ods`, matched
  case-insensitively.
- Discovery walks only configured roots, never follows a directory/file link,
  and deduplicates overlapping roots by file identity.
- Workbook files above the configured byte limit are rejected before Calamine
  is invoked.
- Each ordinary worksheet becomes one `AssetKind::Sheet`; dialog, macro, chart
  and VBA sheets are not returned as tabular assets and produce a sanitized
  discovery warning only when an operation can return warnings.
- Sheet assets are ordered by root order, relative workbook path and workbook
  sheet order.
- Asset UUIDv5 input is the normalized root identity, relative workbook path,
  sheet name and sheet ordinal. Renaming a header does not change the sheet ID.
- Locators contain only root-relative paths and the sheet name. Absolute roots
  never enter an asset, event, warning or public error.

## 6. Container and decoder preflight

- ZIP-based XLSX/XLSM/XLSB/ODS packages are inspected before Calamine. Reject an
  entry count, declared expanded size, individual entry size, invalid path,
  encrypted entry or compression accounting overflow outside configured bounds.
- ODS repeated row/column declarations are checked against the Stillflow sheet
  cell limit before decoding. The upstream Calamine 100-million-cell safety cap
  remains a second line of defense, not the product bound.
- XLS receives the workbook-file byte bound and Calamine structural validation.
  Every decoded sheet range is rejected immediately if its checked area exceeds
  `maxSheetCells`.
- A malformed package, unsupported encryption, corrupt workbook, invalid
  dimension, or excessive decoded range returns sanitized `InvalidData`.
- No raw Calamine, ZIP/XML, absolute-path, formula, or cell-value error text is
  copied into a public error.

## 7. Deterministic region analysis

Analysis considers at most the configured row and column limits. It records
`analysis_truncated = true` if the used range extends beyond that window.

1. Mark non-empty cells in the bounded analysis window. Formula cells use their
   cached value for occupancy but are counted separately.
2. Split row bands at fully empty rows.
3. Within each row band, split column bands at fully empty columns.
4. Trim every rectangular cross-product to its first/last non-empty row and
   column, discard empty results, deduplicate, then order by start row, start
   column, end row and end column.
5. Stop at `maxRegionCandidates`, mark truncation and add a warning; never pick
   one ambiguous region silently.

Region confidence is `High` when the rectangle has at least two data rows and
no internal fully empty row/column, `Medium` when it has data but is sparse, and
`Low` for title-only/single-row candidates.

For each region, score the first five non-empty rows as header candidates:

- 40 points: every non-empty cell is text and at least two cells are present.
- 20 points: all non-empty header values are unique.
- 20 points: at least half of populated columns change from text in the
  candidate to a non-text dominant type in the next ten non-empty rows.
- 10 points: populated-cell coverage is at least 75 percent of region width.
- 10 points: the row does not intersect a merged region.

Scores are clamped to 100: `High >= 80`, `Medium >= 50`, otherwise `Low`.
Inspection always returns the score; it never promotes row zero by convention.

## 8. Formula, merge and hidden metadata

- `worksheet_formula` is used only for formula presence/count and coordinate
  reporting. Formulas are never executed or returned in public messages.
- Cached cell values are the tabular values. A finding states that cached
  formula results may be stale when formulas are present.
- Merged ranges are returned where Calamine exposes them (XLS/XLSX/XLSM).
  Unsupported formats get a stable `workbook.merge_metadata_unavailable`
  informational finding rather than fabricated empty certainty.
- Sheet visibility is always surfaced. Hidden or very-hidden sheets get warning
  findings.
- Hidden row/column metadata is reported where the format adapter can obtain it.
  Otherwise the inspection result uses empty lists plus a stable
  `workbook.hidden_metadata_unavailable` informational finding.

## 9. Explicit selection and schema inference

- `inspect` accepts a discovered sheet without a region selection and returns
  candidates. If a selection is present, its range and header must be one of the
  sheet bounds; otherwise reject it.
- `preview` and `read_batches` require `workbook_region`. The connector never
  selects a region or header on behalf of the caller.
- `NoHeader` yields deterministic `column_1`, `column_2`, ... names. `Row(n)`
  requires `n` inside the selected range and excludes that row plus all earlier
  selected rows from data.
- Empty/duplicate header cells are deterministically replaced or suffixed; the
  original cell text is preserved only in logical field metadata, never errors.
- Column IDs are UUIDv5 over sheet asset ID plus source column coordinate, so
  batch size, projection and display-name repair do not change identity.
- Field metadata records sheet name, zero-based source column, A1 column label,
  selected range start/end and optional header row. Combined with row order,
  every output cell maps deterministically back to a workbook coordinate.

Type inference scans at most 100,000 selected data rows and follows this lattice:

- empty -> nullable null observation
- bool -> Boolean
- integer -> Int64
- float or integer+float -> Float64
- Excel datetime -> millisecond timestamp without timezone
- string, ISO datetime/duration, cell error, or incompatible mixed values -> Utf8

Schema overrides use the existing validated request fields. Conversion failure
after inference/override is `SchemaDrift`; values are never included in errors.

## 10. Preview and streaming

- Only `SamplingStrategy::Head` is supported.
- Predicate pushdown, checkpoints and random/reservoir sampling return
  `UnsupportedCapability`.
- Ordered projection is applied before Arrow builders allocate output columns.
- Preview enforces the existing 1–10,000 row and 1 byte–50 MiB decoded Arrow
  bounds independently and reports independent truncation flags.
- Read batch size remains 1–65,536 rows. Oversized variable-width rows are
  rejected before a public envelope can exceed 64 MiB.
- All record batches use Arrow 59 and `BatchEnvelopeFactory`. Schema pointer
  identity is stable across batches from one prepared reader.
- Dropping a stream releases the workbook range, file handle and builders.
- Cancellation/deadline checks occur before filesystem access, during package
  preflight, between analyzed rows, between decoded output rows and immediately
  before every yield.

Calamine materializes worksheet ranges. The explicitly configured
`maxSheetCells`, package expansion bounds and workbook-byte bound are therefore
part of the operator-state memory contract; Arrow batches remain independently
bounded by the public envelope limit.

## 11. Errors and findings

Use the shared error taxonomy:

- invalid connection/selection/projection/override -> `InvalidConfiguration`
- missing file/sheet -> `NotFound`
- link traversal or unreadable root -> `Authorization`
- corrupt/encrypted/excessive workbook or malformed cell encoding -> `InvalidData`
- post-inference conversion mismatch -> `SchemaDrift`
- deadline/cancel -> `Timeout` / `Cancelled`
- transient file I/O after a successful open -> `TransientSource`

Findings use stable codes and generic messages. They may contain sheet names and
A1 coordinates because those are source locators, but never formulas, cell
values, configured absolute roots, credentials, raw parser text or archive paths.

## 12. Required tests

Unit tests:

- configuration/default/bound validation and secret-field rejection
- coordinate/range validation and serde compatibility
- deterministic UUIDs, ordering and column IDs
- region splitting, trimming, ambiguity and candidate cap
- exact header scoring including no-header, title-only and merged-header cases
- type inference/promotion and sanitized drift
- projection order and schema pointer reuse
- preview row/byte partition invariance
- cancellation/deadline checks

Fixture/integration tests:

- XLS, XLSX, XLSM, XLSB and ODS open through Calamine 0.35
- multi-sheet discovery and independent selection/preview
- empty, title-only, mixed-type, formula, merged-cell and hidden-sheet cases
- corrupt, encrypted and oversized package rejection
- traversal, symlink and outside-root rejection
- ODS repeated-cell expansion rejected at the Stillflow bound
- dropped streams release resources
- no production `unwrap`/`expect`; `unsafe` is denied

CI gates:

- frontend `npm ci`, `npm run typecheck`, `npm run build`
- Rust 1.85.0 and current stable:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`

## 13. Non-goals

- Formula execution or recalculation
- Workbook styling preservation or write support
- Password decryption
- Images, charts, macros or VBA extraction
- Remote/object-store workbook reads
- Polars cleaning, DuckDB materialization, API or storage changes
- Frontend source, layout, CSS or design-token changes

## 14. Merge gate

The PR remains draft until the implementation, fixtures, completion report,
architecture review and full CI matrix are complete. Any deviation from this
contract must be documented in the PR and Issue #7 before merge.
