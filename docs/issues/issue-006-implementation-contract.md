# Issue #6 Implementation Contract: local tabular ingestion

> Status: Frozen
> Risk: High
> Contract issue: #16
> Implementation issue: #6
> Required base: accepted PR1 contracts, rebuilt from latest `main`
> Accepted rebuild base: `main@8a6810e7dc1f95aa31288a97b2e146069eb61ff7`
> Last updated: 2026-08-12

## 1. Objective

Implement one production adapter for bounded discovery, inspection, preview, and
streaming reads of local CSV, TSV, JSON, NDJSON, and Parquet files. The adapter
must implement the existing `SourceConnector` boundary, use the stable logical
contracts from Issue #23, and preserve Arrow 59 as the execution interchange.

This document supersedes historical Issue #6 contract drafts. Historical branches
remain read-only references and must not be merged or cherry-picked.

## 2. In scope

- New crate `stillflow-connector-local-tabular`.
- Typed local-file connection configuration.
- Recursive, bounded, deterministic asset discovery.
- Logical schema inspection and bounded inference.
- Bounded head preview.
- Streaming Arrow batches with requested batch size.
- Projection at the earliest format scan layer.
- Strict schema-drift, malformed-data, path, cancellation, and deadline behavior.
- CSV, TSV, JSON, NDJSON, and Parquet fixtures and integration tests.
- An isolated Polars/Arrow 59 bridge through the Arrow C Data Interface.
- Reuse of one validated `BatchEnvelopeFactory` and canonical Arrow `SchemaRef`
  for every established output schema.

## 3. Non-goals

- Excel/workbooks, XML, YAML, Avro, ORC, compressed archives, URLs, stdin, object
  stores, databases, or document extraction.
- Symbolic links, junctions, Windows reparse points, or hard-link trust policies.
- Character-set conversion beyond UTF-8 with an optional UTF-8 BOM.
- Lenient row skipping, automatic repair, implicit string fallback, or rejected
  row datasets.
- Predicate pushdown in this connector version.
- Random/reservoir preview sampling.
- Incremental checkpoints or change tracking.
- Cleaning-rule execution, DuckDB preview, snapshot persistence, API, or UI.
- Changes to `SourceConnector` or `BatchEnvelope` without a new contract.

## 4. Supported formats

| Format | Extensions | Required shape | Inspection | Projection |
| --- | --- | --- | --- | --- |
| CSV | `.csv` | RFC-style records under explicit dialect | bounded inference | scan layer |
| TSV | `.tsv` | tab-delimited records | bounded inference | scan layer |
| JSON | `.json` | one top-level array of objects | streaming bounded inference | decode layer |
| NDJSON | `.jsonl`, `.ndjson` | one object per non-empty line | bounded inference | decode layer |
| Parquet | `.parquet` | valid header/footer metadata | footer only | row-group scan |

Extensions are matched case-insensitively. Extension identifies the candidate
format; content validation is mandatory before rows are returned. Parquet requires
`PAR1` at both required positions and a valid footer. Text formats accept UTF-8
and strip one leading UTF-8 BOM; all other invalid UTF-8 is `InvalidData`.

JSON objects may contain nested lists/objects only when inference yields one
stable logical shape. A top-level scalar/object, a non-object array element, or
incompatible nested shapes are `InvalidData`/`SchemaDrift`, never stringified.

Empty CSV/TSV, `[]`, and empty NDJSON are valid zero-row sources. They produce an
empty schema unless an authorized schema override supplies fields.

## 5. Configuration

`SourceConnection.config()` is parsed into a private, deny-unknown-fields adapter
configuration. It contains no credential values.

```text
allowedRoots: non-empty list of absolute paths
maxDiscoveryDepth: default 16, range 0..=64
maxDiscoveredAssets: default 10,000, range 1..=100,000
schemaInference.maxRows: default 10,000, range 1..=100,000
schemaInference.maxBytes: default 8 MiB, range 1..=64 MiB
csv.delimiter: default comma, one ASCII byte excluding quote/newline/NUL
csv.quote: default double quote, one ASCII byte
csv.hasHeader: default true
tsv.hasHeader: default true
```

Allowed roots must exist, be directories, be absolute, contain no symlink/reparse
component, and be pairwise de-duplicated after platform-normalized comparison.
Overlapping roots are allowed, but a discovered file appears once under the most
specific root.

The existing `CredentialRef` is a non-secret local capability reference. The
adapter never resolves it to a password or token.

## 6. Request contracts

The PR may add a generic optional logical schema override to `PreviewRequest` and
`ReadRequest` if it does not already exist. An override is a full validated
`LogicalSchema`; it does not live inside connection JSON. Unknown source fields,
missing non-nullable fields, and incompatible values are typed schema errors.

Projection uses ordered `ColumnId` values from the inspected logical schema.
Duplicate or unknown IDs are invalid configuration. Output columns retain request
order. An absent projection returns all columns in source order.

A non-empty filter is rejected with `UnsupportedCapability(predicate_pushdown)`.
It is never evaluated after a full scan. `SamplingStrategy::Head` is supported;
Random and Reservoir return `UnsupportedCapability`.

## 7. Path security

All locators returned by this connector are root-relative slash-separated paths;
absolute host paths are never exposed as asset locators or user-facing errors.

For discovery:

1. Open an allowed root and enumerate entries in lexical normalized order.
2. Inspect metadata without following links.
3. Skip symlinks, junctions, reparse points, sockets, devices, and unknown types.
4. Traverse directories only within the configured depth.
5. Include regular files with supported extensions only.
6. De-duplicate by platform file identity where available and by normalized
   root-relative locator otherwise.
7. If depth or asset count would truncate results, fail explicitly; never return a
   silent partial list.

For inspect/preview/read:

1. Reject absolute locators, empty components, `.`, `..`, NUL, drive prefixes,
   UNC prefixes, and platform separator ambiguity.
2. Walk every component relative to an allowed-root directory handle without
   following links where the platform API permits it.
3. Re-check the final opened handle as a regular file before parsing.
4. Reject symlink/reparse/unknown components with `InvalidConfiguration`.
5. Never use canonicalization alone as the authorization check because it follows
   links and leaves a check/open race.

Platform code that cannot provide no-follow handle traversal must stop for a
security review; a check-then-open fallback is not silently authorized.

## 8. Discovery semantics

- `test_connection` validates configuration and readable root directory handles.
- A healthy result is `Ok`; unreadable individual children produce a deterministic
  degraded warning only if discovery can safely continue.
- `discover` validates its request and optional parent path under the same path
  algorithm.
- Asset order is `(root precedence, normalized relative path)` and is stable across
  runs over the same directory state.
- Asset IDs are deterministic UUIDv5-style identities derived from connector
  namespace plus normalized root identity and relative locator. If the existing
  UUID feature set cannot create v5 IDs, add the feature explicitly and lock it.
- Discovery does not open file bodies and does not infer schema.
- Checkpoint always returns `None`; the capability remains false.

## 9. Inspection and inference

`inspect` returns a `LogicalSchema`, normalized format name, available size and
modification metadata, optional row count, and sanitized findings.

- Parquet reads footer/metadata only and maps supported physical/logical types.
- CSV/TSV/JSON/NDJSON inference reads at most both configured `maxRows` and
  `maxBytes`.
- Inference evaluates values in input order but joins types with the commutative,
  associative, idempotent logical widening operation from Issue #23.
- A field absent from some sampled objects becomes nullable.
- Column identity is deterministically derived from asset ID and original field
  position/name; repeated inspection of unchanged data yields identical IDs.
- Duplicate headers are `InvalidData`; names are not auto-renamed.
- A sample ending before EOF adds `inspect.schema_inference_truncated`.
- CSV/TSV/JSON/NDJSON row count may be absent; inspection must not scan the full
  file merely to count rows.
- Findings contain codes and safe summaries, not row payloads.

## 10. Preview semantics

Preview validates core limits: 1..=10,000 rows and 1 byte..=50 MiB decoded output.

1. Validate request, capability, locator, projection, and optional override.
2. Open the file with the no-follow path algorithm.
3. Apply projection before constructing unrequested output columns.
4. Decode only enough input to produce bounded head rows plus one-row lookahead
   needed to determine row truncation.
5. Emit Arrow batches no larger than the internal preview batch size (1,024 rows).
6. Stop before decoded Arrow buffer bytes exceed `byte_limit`. If one row alone
   cannot fit, return `InvalidData` without its value.
7. Set row/byte truncation independently and exactly.
8. Cancellation/deadline wins over a successful partial preview.

Decoded byte accounting is the sum of Arrow array buffers in returned batches,
not source file length, JSON text length, allocator capacity, or a guess. Tests
must exercise variable-width values and one-row overflow.

Preview working memory is
`O(inference_bound + preview_batch + largest_supported_value)`, never `O(file)`.

## 11. Streaming read semantics

1. Validate request and batch size (1..=65,536).
2. Validate/open the path and establish the output logical/physical schema once.
   Construct one `BatchEnvelopeFactory` at this point; per-batch one-shot
   `BatchEnvelope` construction is forbidden on the streaming hot path.
3. Apply projection at the earliest format-specific scan/decode layer.
4. Produce Arrow 59 batches with at most `batch_size` rows; only the final batch
   may be smaller.
5. Preserve source row order.
6. Validate every later value against the established schema. Incompatible values
   return `SchemaDrift` and terminate the stream.
7. Check cancellation/deadline before open, during inference, before each blocking
   read, at least every 4,096 decoded rows, and before yielding each batch.
8. Dropping the stream closes file/parser resources promptly.
9. Never collect the complete file or all output batches.

The streaming memory target is
`O(inference_bound + batch_size * projected_row_width + parser_state)`.

## 12. Arrow/Polars boundary

The approved bridge is the Arrow C Data Interface, isolated in one adapter module:

```text
Polars chunk/dataframe
  -> Arrow C Data Interface export
  -> arrow-rs 59 import
  -> RecordBatch payload
```

Rules:

- no transmute, private Polars internals, or version-punned Arrow Rust types;
- ownership/release callbacks are exercised under normal, empty, sliced, and
  early-drop paths;
- conversion is per bounded chunk; no whole-file `collect` is allowed;
- supported type mapping has round-trip fixtures including nulls, variable-width
  values, lists/structs if accepted by JSON, dates, and timestamps;
- adapter failures are sanitized `Internal`/`InvalidData` errors, not panics;
- projection must be demonstrated for all five formats before the capability is
  declared true;
- if the bridge cannot preserve schema/ownership or `O(batch)` behavior, stop and
  reopen the architecture decision. Do not add a second Arrow public version.

## 13. Capabilities

| Capability | Value | Behavior |
| --- | --- | --- |
| schema discovery | true | all five formats |
| preview | true | bounded Head only |
| streaming | true | bounded batches |
| incremental read | false | checkpoint returns `None` |
| predicate pushdown | false | any filter rejected |
| column projection | true | required for all formats |
| range read | false | internal Parquet footer seeks do not promise generic range API |
| change tracking | false | no file watcher/CDC |

Capabilities are static for this adapter. A format-specific fallback may not make
an advertised capability false at runtime.

## 14. Errors and warnings

| Condition | Category | Retryable |
| --- | --- | --- |
| invalid config/locator/link | InvalidConfiguration | false |
| unreadable or missing file | NotFound or Authorization | source-dependent |
| malformed text/JSON/Parquet | InvalidData | false |
| duplicate header/non-object JSON row | InvalidData | false |
| post-inference incompatible value | SchemaDrift | false |
| unsupported sampling/filter | UnsupportedCapability | false |
| elapsed request deadline | Timeout | true |
| cancellation token fired | Cancelled | false |
| transient filesystem IO | TransientSource | true when retry is safe |
| FFI invariant failure | Internal | false |

Malformed-row messages may contain a one-based row/line number, column name, byte
offset, and expected/observed logical type. They must not include raw field
values, full records, absolute paths, or credential references.

Strict mode is the only mode. No row is silently skipped or coerced to Utf8.

## 15. Files and dependencies

Expected additions:

```text
backend/crates/stillflow-connector-local-tabular/
  Cargo.toml
  src/{lib,config,path,discover,inspect,preview,read,bridge,formats/...}.rs
  tests/
backend/tests/fixtures/local-tabular/
```

Expected edits are limited to workspace manifests/lockfile and minimal exports or
generic schema-override fields authorized here. No runtime logic belongs in
`stillflow-connectors` beyond registration wiring.

Polars and format/FFI/path dependencies must be isolated to the new adapter and
exactly represented in `Cargo.lock`. Do not add DuckDB, SQLx, Axum, object_store,
Calamine, or the `arrow` meta crate.

## 16. Test matrix

At minimum, automated tests cover:

1. valid/unknown/secret-bearing configuration;
2. missing, relative, overlapping, and unreadable roots;
3. traversal, absolute, UNC/drive, separator, NUL, symlink, junction/reparse cases;
4. deterministic discovery order/IDs and duplicate-root assets;
5. depth and asset-limit failures without partial success;
6. extension/content mismatch for every format;
7. BOM, invalid UTF-8, duplicate headers, malformed quoted rows;
8. CSV comma and configured delimiter; TSV fixed tab behavior;
9. JSON array shape and NDJSON object-per-line shape;
10. nested stable JSON and incompatible nested drift;
11. Parquet footer-only inspect and corrupt header/footer;
12. bounded inference findings and unknown row counts;
13. stable schema and column IDs across repeat inspection;
14. empty files/array, all-null fields, nullability, dates/timestamps;
15. projection ordering and unknown/duplicate IDs for all five formats;
16. unsupported filter and sampling strategies;
17. row-limit, byte-limit, independent truncation, and oversized single row;
18. batch sizes 1, a middle value, and 65,536; final short batch;
19. post-sample schema drift terminates stream;
20. cancellation/deadline before open, mid-inference, mid-stream, before yield;
21. early stream drop releases handles;
22. Arrow C ownership: empty, null, sliced, chunked, early-drop, error paths;
23. peak-memory regression on a fixture much larger than the configured batch;
24. no source values/absolute paths/secrets in errors or snapshots.

## 17. Acceptance criteria

- All five formats discover, inspect, preview, and stream through one adapter.
- Results expose stable `LogicalSchema` plus Arrow 59 batch payloads.
- Projection is proven at scan/decode level for each format; unselected wide
  columns are not materialized.
- Preview never returns more than the row or decoded-byte bound and reports exact
  truncation.
- Large-file peak memory stays within the documented inference/batch bound with a
  tolerance fixed by the memory test harness, not proportional to file size.
- Malicious paths cannot escape an allowed root or traverse a link.
- Schema inference/order gives repeatable schema and column IDs.
- Changing `batch_size` does not change row values, order, logical schema, or
  terminal error, only batch partitioning.
- Filter/random sampling are rejected before a file scan.
- No production `unwrap`, `expect`, unchecked indexing, raw pointer block outside
  the isolated reviewed FFI adapter, or full-file collect exists.
- Backend format, Clippy, workspace tests, and dedicated integration tests pass in
  GitHub Actions.
- Frontend typecheck/build pass without frontend source changes.

## 18. Implementation sequence

1. Add fixtures and failing path/config tests.
2. Implement typed config and no-follow root/locator handling.
3. Implement deterministic discovery and IDs.
4. Implement logical inference and format validation.
5. Prove the C Data Interface bridge and ownership tests.
6. Implement projection-aware readers format by format.
7. Implement bounded preview and exact byte accounting.
8. Implement streaming, drift, cancellation, deadline, and drop semantics.
9. Complete security, memory, and batch-invariance tests.
10. Run workspace checks and perform contract review.

## 19. Stop conditions

Stop and return to contract review if:

- any format requires whole-file materialization;
- no-follow handle traversal cannot be provided on a supported platform;
- plain JSON cannot be parsed incrementally within the memory bound;
- projection cannot avoid constructing unselected columns for any format;
- Polars/Arrow ownership or type fidelity is ambiguous;
- implementation needs a second Arrow public version;
- an error needs raw source values or absolute paths for diagnosis;
- a public trait/BatchEnvelope/UI/storage change is required;
- a historical branch would need to be merged or cherry-picked.

## 20. Known risks

- Plain JSON arrays require a truly incremental array-element parser; this is a
  mandatory spike before production decoding.
- Platform path APIs differ. Unsupported no-follow semantics are a blocker, not a
  reason to weaken the policy.
- The Arrow C bridge contains unsafe FFI by nature and needs a narrowly scoped
  safety rationale plus ownership tests.
- Strict inference may reject heterogeneous real-world files. Rejected-row or
  coercion policies require a separate product contract.
