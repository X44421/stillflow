# Issue #26 Implementation Contract: versioned batch boundary and Arrow 59 adapters

> Status: Frozen
> Risk: High
> Issue: #26
> Authorized base: `main@25d30600ad0e6bcc39cb77c19f1e076191fe93e2`
> Last updated: 2026-08-07

## 1. Objective

Replace every public raw Arrow `RecordBatch` execution boundary with one
versioned and validated `BatchEnvelope`. Define the canonical conversion between
the stable version 1 `LogicalSchema` contract and Apache Arrow 59 so connectors,
engines, previews, and later storage adapters cannot invent incompatible column
identity or type mappings.

This contract is the PR2 delivery node. It authorizes the breaking migration
described by Issue #26 and rejects a compatibility shim for the temporary raw
batch boundary merged in #5.

## 2. In scope

### `stillflow-core`

- A version 1 `BatchEnvelope` owning one Arrow 59 `RecordBatch` payload.
- Immutable shared ownership of one validated `LogicalSchema` per stream.
- A deterministic, explicitly versioned logical-schema fingerprint used only as
  a fast comparison index.
- Source-asset lineage and zero-based batch sequence metadata.
- Exact row and Arrow-memory byte accounting with hard per-envelope bounds.
- Canonical logical-to-Arrow and Arrow-to-logical schema adapters.
- Typed envelope, mapping, sequence, lineage, schema, and bound errors.
- Batch streams whose successful items are envelopes rather than raw batches.
- Stream wrapping that enforces cancellation, deadlines, lineage, schema
  constancy, and sequence continuity without prefetch.
- Preview batches migrated to envelopes, plus result validation against the
  request's source asset, row limit, and byte limit.

### `stillflow-connectors`

- `RawBatchStream`, `SourceConnector`, and `ConnectorRegistry` compile migration.
- Registry enforcement of preview and stream invariants before data crosses the
  public connector boundary.
- Existing stubs and tests migrated to valid envelopes where they yield data.

### Compile fixes

- Minimal exports, imports, documentation, and tests affected by the public type
  changes above.
- No frontend source change is authorized.

## 3. Explicit non-goals

- Polars or DuckDB lowering, execution, or ownership conversion.
- Connector #6 or any source-format parser.
- SQLite, Parquet, snapshots, manifests, or persistence checksums.
- HTTP, IPC, Arrow IPC, Flight, or JSON serialization of Arrow payload buffers.
- API or frontend behavior.
- Physical plan nodes, optimization, scheduling, or distributed streams.
- A second Arrow version, the `arrow` meta crate, or a new third-party package.
- Changing `LogicalType` version 1 or adding unsupported Arrow logical types.
- Merge or cherry-pick of a historical branch.

## 4. Public contract

Names may be organized into modules, but their behavior must match this section.

### 4.1 Envelope and fingerprint

```rust
pub const BATCH_ENVELOPE_VERSION: u16 = 1;
pub const MAX_BATCH_ROWS: usize = 65_536;
pub const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

pub struct LogicalSchemaFingerprint([u8; 32]);

pub struct BatchEnvelope {
    version: u16,
    schema: Arc<LogicalSchema>,
    schema_fingerprint: LogicalSchemaFingerprint,
    source_asset_id: Uuid,
    sequence: u64,
    row_count: usize,
    byte_count: usize,
    payload: RecordBatch,
}
```

Envelope fields remain private. Construction and access use checked constructors
and read-only accessors. A version-accepting constructor exists for decoding and
tests; the normal constructor always emits version 1.

Required invariants:

- `version == 1`;
- `source_asset_id` is not nil;
- `schema.validate()` succeeds;
- `schema_fingerprint` is derived from that exact logical schema;
- `payload.schema()` equals the canonical Arrow encoding of `schema`;
- `row_count == payload.num_rows()`;
- `byte_count == payload.get_array_memory_size()`;
- `row_count <= MAX_BATCH_ROWS`;
- `byte_count <= MAX_BATCH_BYTES`.

The Arrow byte count is a conservative in-memory bound. Shared or sliced buffers
may be counted more than once; under-counting is forbidden. Zero-row batches are
valid because Arrow uses them for empty typed results, but they receive normal
sequence numbers and remain subject to cancellation and backpressure.

The schema is stored as `Arc<LogicalSchema>` because every batch in a stream
shares one immutable schema. This avoids an `O(batch_count * schema_size)` clone
cost. The Arrow payload keeps Arrow's existing immutable reference-counted buffer
ownership; no additional payload copy is introduced.

### 4.2 Schema fingerprint

Canonical schema bytes are compact UTF-8 JSON from the already validated
`LogicalSchema`. Vectors preserve semantic order and metadata is a `BTreeMap`, so
serialization is deterministic.

The version 1 fingerprint is four independently seeded FNV-1a 64-bit lanes over
the canonical bytes, concatenated in big-endian order. Its algorithm identifier
is `stillflow-schema-fnv1a64x4-v1`.

The fingerprint is not a security or persistence checksum. Any apparent match
must compare the complete `LogicalSchema` before accepting schema equality. No
clock, random value, locale, process state, or unordered map iteration may enter
the calculation.

### 4.3 Canonical Arrow 59 mapping

The supported type mapping is exact:

| Logical type | Arrow 59 type |
| --- | --- |
| `Null` | `Null` |
| `Boolean` | `Boolean` |
| `Int8/16/32/64` | same-width signed integer |
| `UInt8/16/32/64` | same-width unsigned integer |
| `Float32/64` | same-width float |
| `Utf8` | `Utf8` |
| `Binary` | `Binary` |
| `Date32` | `Date32` |
| `Timestamp` | same unit and optional timezone |
| `List(T)` | `List(Field("item", T, nullable=true))` |
| `Struct(fields)` | ordered Arrow struct fields using this same mapping |

Large/view/fixed-size strings, binaries and lists; decimal; date64; time;
duration; interval; map; union; dictionary; run-end encoding; and every other
Arrow type are rejected. There is no silent conversion to UTF-8.

Canonical Arrow metadata keys are:

```text
stillflow.schema.version
stillflow.schema.fingerprint
stillflow.schema.metadata
stillflow.column.id
stillflow.field.metadata
```

Schema and field logical metadata are compact JSON objects stored under their
single reserved keys. User metadata is not flattened into the Arrow metadata
namespace, so it cannot collide with reserved keys. Top-level and nested struct
fields carry their lowercase hyphenated `ColumnId`. List element fields use the
fixed name `item`, nullable `true`, and no column identity because version 1
`LogicalType::List` has no element-field identity or nullability.

For every valid supported schema `S`:

```text
from_arrow(to_arrow(S)) = S
fingerprint(from_arrow(to_arrow(S))) = fingerprint(S)
```

Decoding verifies the declared schema version and fingerprint after rebuilding
the complete logical schema. Invalid UUIDs, JSON metadata, versions, fingerprints,
or physical types return typed errors.

### 4.4 Stream semantics

`BatchItem` becomes `Result<BatchEnvelope, ConnectorError>` and `BatchStream`
remains a pinned, `Send`, pull-based stream.

The request wrapper receives the expected source asset ID. For every successful
item it enforces:

```text
sequence[0] = 0
sequence[i + 1] = sequence[i] + 1
source_asset_id[i] = expected_source_asset_id
schema[i] = schema[0]
```

Fingerprint comparison may reject a mismatch early, but a matching fingerprint
must be followed by full logical-schema equality. Sequence addition uses checked
arithmetic. A violation emits one sanitized terminal error, drops the inner
stream, and yields `None` forever afterward.

The wrapper polls the inner stream at most once for each downstream `poll_next`.
It does not spawn a producer, create a channel, buffer an item, or prefetch. Thus
backpressure is inherited directly from the consumer. Cancellation and deadlines
are checked before polling or yielding an inner item and wake a pending consumer.

### 4.5 Preview semantics

`PreviewData.batches` becomes `Vec<BatchEnvelope>` while its top-level
`LogicalSchema` remains the schema available for a valid empty preview.

Before a connector preview leaves `ConnectorRegistry`, validation must prove:

- top-level schema is valid;
- every envelope has the requested source asset lineage;
- envelope sequences are contiguous from zero;
- every envelope schema equals the top-level schema;
- checked sums of rows and bytes equal `rows_returned` and `bytes_returned`;
- summed rows and bytes do not exceed the request limits.

Truncation flags report source knowledge and are not inferred solely from equality
with a limit. An empty preview has zero envelopes, counts of zero, and a valid
logical schema.

## 5. Complexity and resource laws

Let `C` be the number of logical fields including nested struct fields, `M` the
logical metadata size, and `B` the number of Arrow columns in one record batch.

- schema conversion and envelope construction are `O(C + M + B)`;
- one stream item validation is `O(C + B)` in the collision-safe worst case;
- stream state is `O(schema)` and never grows with the number of batches;
- total live streaming memory is `O(batch_bytes + schema + bounded_operator_state)`;
- preview memory remains bounded by its existing total byte limit;
- no operation collects an unbounded source or the remainder of a stream.

Batch partitioning is not semantic. For any ordered logical row sequence `R` and
two valid partitions `P1` and `P2`:

```text
concat(payloads(P1)) = R = concat(payloads(P2))
schema(P1) = schema(P2)
lineage(P1) = lineage(P2)
```

Only envelope count, sequence values attached to partitions, and per-envelope
row/byte counts may differ.

## 6. Errors and security

- Batch construction and validation return typed errors; they do not panic.
- Unsupported versions and malformed reserved metadata are `InvalidData`.
- A physical/logical mismatch or schema change is `SchemaDrift`.
- A sequence or lineage violation is `InvalidData`.
- Cancellation and deadline categories retain their current behavior.
- Errors may contain versions, counts, limits, sequence numbers, UUIDs, metadata
  key names, and logical/physical type names.
- Errors must not contain source row values, payload debug output, credentials,
  raw SQL, source locators, or full paths.
- No production `unwrap`, `expect`, unchecked index, or unsafe block is authorized.

## 7. Files and dependencies

Expected additions:

```text
backend/crates/stillflow-core/src/batch.rs
docs/issues/issue-026-batch-envelope-implementation-contract.md
```

Expected edits are limited to:

```text
backend/crates/stillflow-core/src/{lib.rs,stream/mod.rs,domain/preview.rs}
backend/crates/stillflow-connectors/src/{lib.rs,connector.rs,raw_batch_stream.rs,registry.rs}
```

Test modules in those files may change. A narrowly necessary additional core or
connector test file is allowed. No manifest or lockfile change is expected; any
new dependency is a stop condition.

## 8. Implementation checklist

1. Add fingerprint, Arrow mapping, typed errors, and exhaustive mapping tests.
2. Add private-field `BatchEnvelope` construction, bounds, and accessors.
3. Change `BatchItem`/`BatchStream` to envelopes.
4. Extend stream wrapping with expected lineage, sequence, and schema checks.
5. Change preview batches to envelopes and add checked aggregate validation.
6. Enforce preview/stream validation in `ConnectorRegistry`.
7. Migrate connector stubs, exports, docs, and tests.
8. Add cancellation, deadline, terminal-error, no-prefetch, early-drop,
   partition-invariance, and invalid-stream tests.
9. Run narrow tests, then all repository checks.
10. Compare the final diff with this contract and report every deviation.

## 9. Acceptance criteria

- `rg 'Result<RecordBatch|Vec<RecordBatch>' backend/crates` finds no public batch
  boundary remnant.
- Envelope constructors reject every invalid invariant in section 4.1.
- Logical/Arrow/logical round trips cover every supported atomic type, timestamps
  with and without timezone, nested list/struct types, metadata, field order,
  nullability, and stable `ColumnId` values.
- Unsupported Arrow types and malformed/missing reserved metadata are rejected.
- Stream tests reject a nonzero first sequence, gaps, duplicates, lineage changes,
  and schema changes, with exactly one terminal error.
- Empty typed batches are accepted.
- Two different valid batch partitions reconstruct identical ordered row values.
- Pending cancellation and deadline tests wake within their test timeout.
- Backpressure tests prove construction does not poll and each downstream poll
  advances the inner stream at most once.
- Early drop releases inner stream state without a background producer.
- Preview tests reject mismatched counts, schema, lineage, sequence, and request
  bounds using checked arithmetic.
- No new third-party dependency, production `unwrap`/`expect`, unsafe block,
  unbounded collect, or frontend source change is introduced.
- Backend format, Clippy, workspace tests, frontend typecheck, and frontend build
  pass in GitHub Actions.

## 10. Stop conditions

Stop and return to contract review if implementation requires:

- a second Arrow version or the `arrow` meta crate;
- an unsupported logical type or a lossy/implicit Arrow conversion;
- mutable/shared schema state or a payload copy per boundary;
- a producer task, channel, prefetch buffer, or unbounded queue;
- changes to Polars, DuckDB, storage, API, or frontend behavior;
- a persisted checksum or claim that the schema fingerprint is cryptographic;
- raw source values, paths, SQL, or credentials in an error;
- a new dependency, compatibility shim, or historical branch integration.

## 11. Known risks

- Arrow array memory accounting is deliberately conservative and may double-count
  shared buffers. This can reject a safe batch but must not accept an oversized
  one.
- Arrow metadata is not a security boundary. Every decoded schema is rebuilt and
  validated rather than trusting its declared fingerprint.
- Version 1 lists cannot preserve a physical element name or element nullability;
  their canonical representation fixes those values explicitly.
- The schema fingerprint is collision-prone by cryptographic standards; full
  schema equality is mandatory on every apparent stream match.
