# Issue #23 Implementation Contract: logical schema, expression AST, and plan DAG

> Status: Frozen
> Risk: High
> Issue: #23
> Authorized base: PR0 branch, rebuilt from the latest accepted `main`
> Last updated: 2026-08-07

## 1. Objective

Establish deterministic, execution-engine-independent contracts before connector
or engine implementations multiply the physical assumptions currently present in
#5. The result is a stable logical schema, a typed expression/rule language, and a
validated logical-plan DAG.

This contract explicitly authorizes breaking the public contracts merged in #5.

## 2. In scope

### `stillflow-core`

- Stable `ColumnId` independent of a field's display name.
- Versioned `LogicalSchema` and `LogicalField`.
- Versioned logical scalar types with a deterministic partial widening operator.
- Serializable `ScalarValue`, unary/binary operators, and `Expr` AST.
- Expression reference validation against a logical schema.
- Replacement of free-form `SourceFilter` text/dialect with a typed expression.
- Replacement of name-based request projections with ordered `ColumnId` values.
- Replacement of public inspection/preview Arrow schema fields with
  `LogicalSchema`; Arrow batch payloads remain unchanged until `BatchEnvelope`.
- Typed validation errors that contain no source values or credentials.

### `stillflow-plan`

- A new workspace crate depending only on `stillflow-core`, `serde`,
  `serde_json`, `thiserror`, and `uuid` already approved by the workspace.
- Serializable cleaning `Rule` nodes.
- Serializable logical plan nodes stored as a DAG.
- Linear-time structural validation.
- Canonical JSON bytes and a deterministic non-security fingerprint.

### Compile fixes

- Update existing core and connector tests/imports affected by removing
  `FilterDialect` and the string expression field.
- Update workspace manifests and the committed lockfile when required.

## 3. Explicit non-goals

- `BatchEnvelope`, Arrow schema conversion, or Arrow payload metadata.
- Polars or DuckDB lowering/execution.
- Local tabular connector implementation.
- SQLite/Parquet persistence.
- HTTP/API changes or any frontend change.
- Plan optimization, cost estimation, distributed scheduling, or physical plans.
- Compatibility shims for the #5 string filter contract.
- Merge/cherry-pick of historical branches.

## 4. Public contract

Names may be organized into modules, but their semantics must match this section.

### 4.1 Stable columns and schemas

```rust
pub struct ColumnId(Uuid);

pub enum LogicalType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date32,
    Timestamp { unit: TimeUnit, timezone: Option<String> },
    List(Box<LogicalType>),
    Struct(Vec<LogicalField>),
}

pub struct LogicalField {
    pub id: ColumnId,
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
    pub metadata: BTreeMap<String, String>,
}

pub struct LogicalSchema {
    pub version: u16,
    pub fields: Vec<LogicalField>,
    pub metadata: BTreeMap<String, String>,
}
```

Required invariants:

- schema version is `1` for this contract;
- `ColumnId` values are unique;
- non-empty field names are unique in a schema;
- field order is meaningful and preserved;
- metadata is ordered and must not contain secrets;
- renaming a field changes only `name`, never `id`;
- construction/rename APIs validate invariants before returning a schema;
- serialized schemas never generate IDs or timestamps.

### 4.2 Type widening

`LogicalType::least_upper_bound(&self, other)` returns a type or a typed
`IncompatibleTypes` error.

For the atomic test domain:

- `Null` is the bottom type;
- identical types return themselves;
- signed integers widen within the signed family;
- unsigned integers widen within the unsigned family;
- `Float32 + Float64 = Float64`;
- any integer plus any float returns `Float64`;
- any signed plus unsigned integer returns `Float64` in this version;
- Boolean, Utf8, Binary, and Date32 combine only with themselves or Null;
- timestamps combine only when timezones are equal and widen to the finer unit;
- lists combine recursively when their elements combine;
- structs combine only when field counts, IDs, and names match; field types widen
  recursively and nullability is the logical OR;
- every other pair is an error, never an implicit conversion to Utf8.

For all supported atomic triples where both sides are defined, tests must verify:

```text
join(a, b) = join(b, a)
join(join(a, b), c) = join(a, join(b, c))
join(a, a) = a
```

### 4.3 Typed expressions

The AST must be closed and serializable. It includes:

- column reference by `ColumnId`;
- null, boolean, signed/unsigned integer, finite float, and UTF-8 literals;
- unary `Not` and numeric `Negate`;
- binary equality/ordering, Boolean AND/OR, arithmetic, and string containment;
- `IsNull`, explicit `Cast`, and `Coalesce` nodes.

Non-finite floating-point literals are rejected at construction. Expressions do
not contain closures, SQL fragments, Polars expressions, DuckDB expressions, or
engine callbacks.

`Expr::referenced_columns()` returns a sorted, de-duplicated set. Validation must
reject unknown column IDs. Static result-type inference beyond the operations
needed for reference validation is not required in this PR.

The connector-facing filter becomes structurally typed:

```rust
pub struct SourceFilter {
    pub expression: Expr,
}
```

`FilterDialect` and the public string expression are removed without a
compatibility shim.

`PreviewRequest.projection` and `ReadRequest.projection` become
`Option<Vec<ColumnId>>`. `AssetMetadata.schema` and `PreviewData.schema` become
`LogicalSchema`. Request validation rejects an explicitly empty projection and
duplicate column IDs. Format adapters validate unknown IDs against the inspected
schema. Raw Arrow `RecordBatch` payloads remain in `PreviewData`/streams for this
delivery and are replaced only by the later `BatchEnvelope` contract.

### 4.4 Rules

The closed `Rule` enum must cover the logical intent of:

- rename;
- cast with explicit failure policy;
- trim;
- literal replace;
- fill null;
- drop column;
- derive column from an expression;
- filter rows;
- deduplicate by ordered key columns;
- validate an expression with severity and message.

Rules reference columns by `ColumnId`. A derive rule contains the stable identity,
name, logical type, and nullability of its output. User-facing messages are data,
not executable expressions.

### 4.5 Plan DAG

```rust
pub struct PlanNodeId(Uuid);

pub struct LogicalPlan {
    pub version: u16,
    pub root: PlanNodeId,
    pub nodes: BTreeMap<PlanNodeId, PlanNode>,
}
```

The closed node kinds are:

- `Scan` — zero inputs, stable source asset ID, ordered projection, optional Expr;
- `Project` — one input, ordered columns;
- `Filter` — one input, Expr;
- `ApplyRules` — one input, ordered rules;
- `Join` — exactly two positional inputs, join type and ordered key expressions;
- `Union` — at least two positional inputs;
- `Materialize` — one input and a logical output label only.

Structural validation must reject:

- version other than `1`;
- absent root;
- input references absent from `nodes`;
- self edges or any directed cycle;
- node arity that violates the table above;
- empty required projections, rules, join keys, or labels;
- duplicate IDs inside a projection or deduplication key.

DAG validation uses depth-first coloring or Kahn's algorithm in `O(V + E)` time
and `O(V)` auxiliary memory. Recursive traversal must not be used if it can
overflow on an adversarial plan; an iterative algorithm is preferred.

## 5. Deterministic serialization and fingerprint

Canonical bytes are compact UTF-8 JSON produced only after successful validation.

- maps are `BTreeMap` and serialize in key order;
- vectors retain semantic order;
- UUIDs use their standard lowercase hyphenated representation;
- enum representation is explicit and versioned;
- no clock, process, locale, random generator, or hash-map iteration participates;
- serialization itself does not normalize or mutate a plan.

The fingerprint is a deterministic, explicitly versioned 256-bit cache index
derived from the canonical bytes without a new cryptographic dependency. It is
not an integrity or security checksum. Cache hits must compare canonical bytes
before reuse, so a fingerprint collision cannot change semantics. A later storage
contract may introduce BLAKE3/SHA-256 for persisted-content integrity.

## 6. Error and security semantics

- Constructors and validators return typed errors; they do not panic.
- Errors may include IDs, field names, node kinds, and type names.
- Errors must not include source row values, credentials, raw SQL, or full source
  paths.
- No production-path `unwrap` or `expect` is authorized.
- Test-only `unwrap`/`expect` is allowed and must be reported in the PR.

## 7. Implementation checklist

1. Add logical schema and expression modules to `stillflow-core`.
2. Add constructors, invariant validation, rename/lookup, and widening.
3. Replace `SourceFilter`, projections, and public metadata schemas; remove
   `FilterDialect` and fix compile consumers.
4. Create `stillflow-plan` and register it in the workspace.
5. Implement rule types and their local validation.
6. Implement plan nodes and iterative DAG/arity validation.
7. Implement canonical bytes and versioned fingerprint.
8. Add exhaustive atomic widening-law tests and nested edge cases.
9. Add expression serialization/reference-validation tests.
10. Add missing-root/reference/arity/cycle and deep-DAG plan tests.
11. Add insertion-order-independent canonicalization/fingerprint fixtures.
12. Run all required checks and complete the PR report.

## 8. Acceptance criteria

- `stillflow-plan` depends on no workspace crate except `stillflow-core`.
- `rg 'FilterDialect|expression: String' backend/crates` finds no public filter
  contract remnants.
- Rename tests prove `ColumnId` stability.
- Duplicate ID/name and invalid version cases return errors.
- Exhaustive atomic widening laws pass; incompatible pairs return typed errors.
- Non-finite float literals are rejected.
- Unknown expression columns are rejected without exposing source values.
- All invalid DAG classes in section 4.5 have unit tests.
- A chain of at least 10,000 nodes validates without recursive stack overflow.
- Two plans with identical content inserted in different map order have identical
  canonical bytes and fingerprints.
- Changing an expression, ordered input, or rule changes canonical bytes.
- No Polars, DuckDB, SQLx, Axum, or Arrow dependency is added to
  `stillflow-plan`.
- Backend format, Clippy, and workspace tests pass in GitHub Actions.
- Frontend typecheck/build remain unchanged and pass in GitHub Actions.

## 9. Stop conditions

Stop for contract review if implementation requires:

- an engine-specific logical type or expression node;
- implicit fallback to string/SQL expressions;
- recursive validation that fails the deep-DAG test;
- a dependency beyond the approved list;
- `BatchEnvelope`, persistence, API, or frontend behavior;
- a compatibility shim not specified here;
- a nondeterministic collection or serialization input.

## 10. Known risks

- The v1 widening lattice deliberately promotes signed/unsigned mixtures to
  `Float64`; exact decimal semantics require a later versioned extension.
- A non-cryptographic fingerprint is only an index. Canonical-byte equality is
  mandatory before cache reuse.
- Logical/Arrow conversion is deferred, so physical schema drift remains outside
  this PR and must be solved by the BatchEnvelope contract.
