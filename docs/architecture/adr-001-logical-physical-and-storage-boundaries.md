# ADR-001: Separate logical, physical, and storage boundaries

> Status: Accepted
> Date: 2026-08-07
> Decision owners: Stillflow maintainers

## Context

Stillflow must ingest heterogeneous sources, apply reproducible cleaning rules,
serve bounded interactive previews, and persist auditable snapshots. Polars,
DuckDB, Arrow, SQLite, and Parquet overlap in capability, so an undefined boundary
would create duplicate rule semantics, engine-specific public types, unstable
serialization, and unbounded data movement.

The contracts merged in #5 established the connector foundation but used
physical Arrow schemas and free-form filter strings in places where a stable
logical model is required. The maintainers have explicitly authorized a breaking
migration before downstream implementations depend on those forms.

## Decision

### Logical plane

`stillflow-core` owns stable domain identities, logical schemas, logical scalar
types, and typed expressions. `stillflow-plan` owns rule nodes, logical-plan DAGs,
validation, canonicalization, and deterministic fingerprints.

Logical contracts must not contain Polars, DuckDB, SQLx, Axum, or physical Arrow
buffer objects. Column identity is independent of a mutable display name.

### Execution plane

Apache Arrow 59 is the bounded columnar interchange protocol. A later, versioned
`BatchEnvelope` contract will carry logical schema identity, batch sequence,
lineage, and physical `RecordBatch` payloads.

Polars is the sole canonical executor for cleaning and transformation rules.
DuckDB owns bounded preview SQL, federation, joins, comparison, and temporary
materialization. A DuckDB operation must not define a second interpretation of a
cleaning rule.

### Persistence plane

SQLite stores transactional control-plane metadata: objects, sessions, plans,
jobs, lineage, events, snapshot manifests, and schema versions. Immutable Parquet
partitions store materialized tabular data. A snapshot becomes visible only after
all partitions and checksums are durable and its SQLite manifest is committed
atomically.

SQLite and Parquet types are adapter concerns. Persisted formats are versioned and
must be migrated explicitly; they do not leak into logical contracts.

### Dependency direction

```text
stillflow-api -> stillflow-engine
stillflow-engine -> stillflow-plan, stillflow-connectors, stillflow-storage
stillflow-plan -> stillflow-core
stillflow-connectors -> stillflow-core
stillflow-storage -> stillflow-core
```

The storage crate is introduced when its contract is implemented. Until then,
engine and API code may not absorb persistence responsibilities as a shortcut.

## Invariants

1. A logical plan is deterministic and execution-engine independent.
2. Equivalent logical plans have identical canonical bytes and fingerprints.
3. Schema widening is an explicit, tested operation rather than parser behavior.
4. Streaming memory is bounded by batch size plus explicitly bounded operator
   state.
5. Preview always has row and byte limits and reports truncation.
6. Snapshot publication is atomic; readers never observe a partial snapshot.
7. Secrets are references only and never enter logical plans, events, or snapshot
   manifests.

## Consequences

### Benefits

- Cleaning semantics remain portable and testable without running an engine.
- Connectors and engines can evolve independently behind Arrow adapters.
- Plan caching, replay, lineage, and snapshot identity have stable inputs.
- Preview performance can use DuckDB without changing canonical rule results.
- Metadata transactions and analytical files use formats suited to their jobs.

### Costs

- Logical-to-physical type conversion requires explicit adapters and tests.
- Polars and DuckDB schema parity must be maintained at their shared boundary.
- Breaking the #5 filter/schema surface requires downstream compile fixes.
- Snapshot publication needs staging, checksums, recovery, and garbage collection.

## Rejected alternatives

- **Expose Polars DataFrames publicly:** couples every connector and API to one
  physical engine and makes bounded streaming harder to enforce.
- **Use DuckDB for cleaning too:** creates two rule languages and semantic drift.
- **Store all data in SQLite:** unsuitable for large analytical columnar scans.
- **Store all metadata in Parquet:** loses simple transactional updates and
  relational integrity for the control plane.
- **Serialize engine expressions as strings:** prevents structural validation,
  safe pushdown, deterministic migration, and precise lineage.
- **Adopt historical branches as implementation bases:** obscures accepted state
  and imports assumptions made before the current decisions.

## Verification

This decision is enforced by crate dependency checks, typed public contracts,
logical-law tests, canonical serialization fixtures, batch-bound tests, engine
parity tests, and storage atomicity/recovery tests in their respective delivery
nodes.
