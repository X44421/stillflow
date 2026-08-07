# Issue #30 Implementation Contract: batch-boundary hardening

> Status: Frozen
> Risk: High
> Issue: #30
> Authorized base: `main@a06967ea5b1ce6037e155528bb44cd57349b05fd`
> Last updated: 2026-08-07

## 1. Objective

Harden the version 1 logical-schema and `BatchEnvelope` boundary merged by PR
#27 before PR3 snapshot storage depends on it. Close the request-cancellation
race, impose explicit schema resource ceilings, make canonical Arrow decoding
strict, provide a reusable validated envelope factory, and verify both the
declared Rust MSRV and current stable toolchain.

This contract is a PR2.1 correction node. It does not change the delivery order
or pull storage behavior forward.

## 2. Authorized public changes

### `stillflow-core`

Add the following version 1 resource constants:

```rust
pub const MAX_SCHEMA_NESTING_DEPTH: usize = 64;
pub const MAX_SCHEMA_FIELDS: usize = 4_096;
pub const MAX_SCHEMA_TEXT_BYTES: usize = 1024 * 1024;
```

Schema text bytes are the checked sum of UTF-8 byte lengths for:

- every logical field name;
- every non-empty timestamp timezone;
- every schema and field metadata key and value.

Add typed `LogicalError` variants carrying structural counts and limits only:

- nesting depth exceeded;
- total logical field count exceeded;
- cumulative schema text bytes exceeded.

Add typed `BatchError` variants for non-canonical Arrow schema and field
metadata.

Add one public reusable factory:

```rust
pub struct BatchEnvelopeFactory {
    version: u16,
    schema: Arc<LogicalSchema>,
    schema_fingerprint: LogicalSchemaFingerprint,
    arrow_schema: SchemaRef,
    source_asset_id: Uuid,
}
```

The exact accessor names may follow existing style, but construction and behavior
must match section 5.

### Behavioral tightening

Previously accepted schemas above the new limits and non-canonical Arrow metadata
representations become invalid. This is an intentional validation tightening, not
an envelope-version or logical-type-version change. No compatibility shim is
provided for invalid or non-canonical inputs.

The following remain unchanged:

- `LOGICAL_SCHEMA_VERSION == 1`;
- `BATCH_ENVELOPE_VERSION == 1`;
- the supported logical/Arrow type matrix;
- `stillflow-schema-fnv1a64x4-v1`;
- the per-envelope 65,536-row and 64-MiB Arrow-memory ceilings;
- collision-safe full logical-schema equality after fingerprint comparison.

## 3. In-scope files

Expected addition:

```text
docs/issues/issue-030-batch-boundary-hardening-contract.md
```

Expected edits are limited to:

```text
backend/crates/stillflow-core/src/logical.rs
backend/crates/stillflow-core/src/batch.rs
backend/crates/stillflow-core/src/lib.rs
backend/crates/stillflow-core/src/stream/mod.rs
.github/workflows/ci.yml
```

Test modules colocated in those Rust files may change. No manifest, lockfile,
connector, engine, storage, API, or frontend source change is expected.

## 4. Schema resource laws

### 4.1 Bounded iterative validation

`LogicalType::validate`, `LogicalField::validate`, and
`LogicalSchema::validate` must enforce the same limits. Validation of nested
`List` and `Struct` shapes must use an explicit work stack rather than one
Rust call-stack frame per nesting level.

For a schema with `C` fields, `T` logical type nodes, and `M` cumulative text
bytes:

- time is `O(C + T + M)`;
- auxiliary memory is `O(C + T)`, bounded by the constants above;
- validation performs no unbounded source read or payload collection;
- all additions use checked arithmetic;
- an over-limit input returns the matching typed error before Arrow conversion or
  fingerprint serialization.

The existing sibling-level duplicate field-ID and field-name rules remain
unchanged. This contract does not redefine nested column-identity scope.

### 4.2 Depth definition

An atomic top-level field type has depth 1. Entering a `List` element or a
nested `Struct` field type increases depth by one. Empty top-level schemas have
depth zero. Depth 64 is accepted and depth 65 is rejected.

### 4.3 Exact-limit behavior

The exact field and text limits are inclusive. The first field or UTF-8 byte
above a limit returns its specific typed error. Timestamp timezone bytes count
once. Metadata text bytes count key and value bytes, not JSON punctuation.

Mutation helpers such as rename and `with_metadata` must not leave a value in a
partially updated invalid state when a new limit would be exceeded.

## 5. Reusable envelope factory

### 5.1 Construction

Factory construction receives one `Arc<LogicalSchema>`, one source-asset ID,
and optionally the explicit envelope version for tests/decoding.

It must:

1. reject unsupported envelope versions;
2. reject a nil source-asset ID;
3. validate the logical schema once;
4. compute the versioned fingerprint from the already validated schema;
5. construct the canonical Arrow `SchemaRef` once;
6. retain immutable shared ownership of both schemas.

Factory construction is `O(C + T + M)`.

### 5.2 Envelope construction

For each sequence and `RecordBatch`, the factory must:

- require exact equality between `payload.schema()` and its retained canonical
  Arrow schema;
- compute exact row and conservative Arrow-memory byte counts;
- enforce the existing per-envelope limits;
- build a private-field `BatchEnvelope` using the factory's shared logical
  schema, fingerprint, version, and source lineage;
- introduce no Arrow payload-buffer copy.

Per-envelope factory construction is `O(B)`, where `B` is the number of Arrow
arrays/columns inspected by Arrow memory accounting.

Two envelopes from one factory must satisfy:

```text
Arc::ptr_eq(envelope[0].shared_schema(), envelope[1].shared_schema())
envelope[0].schema_fingerprint() = envelope[1].schema_fingerprint()
envelope[0].source_asset_id() = envelope[1].source_asset_id()
```

The factory exposes read-only access to the canonical Arrow `SchemaRef` so a
connector can construct matching `RecordBatch` values without rebuilding
metadata.

Existing `BatchEnvelope::try_new` and `try_from_parts` remain source
compatible and may create a one-shot factory internally.

## 6. Strict canonical Arrow decoding

`logical_schema_from_arrow` remains the strict inverse of the canonical encoder,
not a tolerant normalizer.

A canonical top-level Arrow schema contains exactly:

```text
stillflow.schema.version
stillflow.schema.fingerprint
stillflow.schema.metadata
```

A canonical logical field contains exactly:

```text
stillflow.column.id
stillflow.field.metadata
```

List element fields retain their already frozen fixed name, nullability, and
empty metadata.

Decoding must reject:

- any missing or additional schema/field metadata key;
- version text other than its minimal base-10 representation;
- fingerprint text other than the exact lowercase 64-character canonical value;
- column UUID text other than the exact lowercase hyphenated representation;
- schema or field metadata JSON whose input bytes differ from compact JSON of the
  decoded ordered map;
- every malformed value already rejected by the PR2 contract.

After rebuilding and validating the complete `LogicalSchema`, the decoder must
recompute the fingerprint and compare both its value and canonical text. Arrow
metadata remains untrusted and is not an integrity boundary.

## 7. Cancellation and deadline linearization

For one downstream `poll_next`, the validated wrapper must:

1. return `None` immediately when already terminated;
2. check request activity;
3. register and poll cancellation/deadline wakeups;
4. poll the inner stream no more than once;
5. validate a successful envelope;
6. check request activity again immediately before yielding a successful item.

If cancellation or deadline becomes terminal during inner polling or envelope
validation, the wrapper emits exactly one terminal `Cancelled` or `Timeout`
error, drops the inner stream, and returns `None` forever afterward. The request
error takes precedence over a simultaneously ready successful envelope.

The wrapper still must not spawn, channel, buffer, prefetch, or poll the inner
stream more than once per downstream call.

## 8. CI toolchain gate

The backend workflow must run its complete format, Clippy, and workspace-test
sequence on both:

- Rust `1.85.0`, matching `rust-toolchain.toml` and
  `workspace.package.rust-version`;
- current stable, detecting forward-compatibility regressions.

Cache identity must remain toolchain-specific. Both backend jobs are required
evidence; neither may be reported as the other.

Frontend `npm ci`, typecheck, and build remain unchanged.

## 9. Errors and security

- Every new error contains only depth, field, text-byte, or metadata-scope
  information.
- No error contains row values, payload debug output, credentials, SQL, locators,
  paths, or raw metadata values.
- Schema and Arrow validation return typed errors; they do not panic.
- No production `unwrap`, `expect`, unchecked index, unsafe block, or
  recursion proportional to external schema depth is authorized.
- Fingerprints remain non-cryptographic comparison indexes.

## 10. Explicit non-goals

- SQLite, Parquet, snapshot manifests, publication, recovery, tombstones, or GC.
- Polars/DuckDB lowering or any engine implementation.
- Connector #6 or source-format parsing.
- API transport, jobs, frontend behavior, layout, CSS, or tokens.
- New logical/Arrow types, decimal semantics, or a new fingerprint version.
- Persisted-content checksums.
- Producer tasks, channels, queues, prefetch, buffering, or background work.
- New third-party dependencies.
- Historical branch merge or cherry-pick.

## 11. Implementation checklist

1. Refactor logical validation to one bounded iterative traversal.
2. Add exact-limit, over-limit, and deep-shape tests.
3. Add validated fingerprint/Arrow-schema helpers and the reusable factory.
4. Preserve direct envelope constructors through the one-shot factory path.
5. Tighten Arrow schema/field metadata decoding and add negative fixtures.
6. Add the post-poll request check and deterministic cancellation/deadline tests.
7. Export only the authorized factory, constants, and errors.
8. Split backend CI into exact-MSRV and current-stable jobs.
9. Run narrow tests followed by all repository checks.
10. Compare the final diff with this contract and report every deviation.

## 12. Acceptance criteria

- Atomic top-level depth 1 and nested depth 64 validate; depth 65 returns the
  nesting-limit error without stack overflow.
- Exactly 4,096 total logical fields validate; field 4,097 returns the
  field-limit error.
- Exactly 1 MiB of counted schema text validates; one additional byte returns the
  text-limit error.
- Existing duplicate field ID/name, unsafe metadata, empty name/timezone, and
  unsupported version behavior remains intact.
- Rename or metadata mutation that exceeds a bound returns an error without
  leaving the schema/field partially mutated.
- Strict Arrow decoding rejects extra keys and non-canonical version, UUID,
  fingerprint, and JSON forms.
- Canonical logical -> Arrow -> logical round trips remain equal for the complete
  supported type matrix.
- Two envelopes built by one factory share the logical-schema allocation,
  fingerprint, source lineage, and canonical Arrow-schema allocation.
- Direct envelope constructors preserve their existing behavior.
- An inner stream that cancels its token and returns a valid envelope in the same
  poll yields one `Cancelled` error and no envelope.
- An inner stream that crosses its deadline and returns a valid envelope in the
  same poll yields one `Timeout` error and no envelope.
- Pending cancellation/deadline wakeups, one-terminal-error behavior,
  backpressure, no-prefetch, early-drop, and partition invariance remain green.
- No new dependency, raw public `RecordBatch` boundary, payload copy, unbounded
  collect, production `unwrap`/`expect`, unsafe block, or frontend change is
  introduced.
- Rust 1.85.0 backend format/Clippy/tests, current-stable backend
  format/Clippy/tests, frontend typecheck, and frontend build all pass.

## 13. Stop conditions

Stop and return to contract review if implementation requires:

- changing a version number, fingerprint algorithm, type mapping, or envelope
  lineage model;
- changing duplicate-ID scope or other logical semantics beyond resource bounds;
- accepting rather than rejecting non-canonical Arrow metadata;
- a payload copy, producer task, channel, prefetch buffer, or unbounded queue;
- a new dependency or manifest/lockfile change;
- storage, connector, engine, API, or frontend behavior;
- historical branch integration.

## 14. Known risks

- The direct one-shot envelope constructors remain `O(C + T + M + B)`; high
  throughput producers must retain and reuse the factory.
- The constants are version-1 policy. Raising them later requires resource
  profiling and a contract update.
- Strict decoding may reject third-party Arrow metadata that a tolerant adapter
  would ignore. External schemas must be explicitly normalized before entering
  this canonical boundary.
- A cancellation can always occur after the final activity check; that check is
  the documented linearization point for successful yield.
