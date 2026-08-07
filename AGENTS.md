# Stillflow — Agent Instructions

These rules apply to automated review and implementation in this repository.
The accepted architecture is in
[`docs/data-ingestion-architecture.md`](docs/data-ingestion-architecture.md), and
the delivery workflow is in
[`docs/development/ai-development-workflow.md`](docs/development/ai-development-workflow.md).

## Sources of truth

1. The latest `main` branch is the only implementation base.
2. Historical branches are read-only references. Do not merge or cherry-pick them.
3. A frozen Implementation Contract may authorize a deliberate exception to these
   rules. Record the exception in the PR body.

## Dependency direction

```text
stillflow-api -> stillflow-engine
stillflow-engine -> stillflow-plan, stillflow-connectors, stillflow-storage
stillflow-plan -> stillflow-core
stillflow-connectors -> stillflow-core
stillflow-storage -> stillflow-core
stillflow-core -> no workspace crate
```

`stillflow-storage` is an accepted target boundary and may not exist yet. No
dependency may point from a lower layer back to a higher layer.

## Frozen engineering rules

1. `stillflow-core` contains stable domain contracts only. It must not depend on
   Polars, DuckDB, SQLx, Axum, or an adapter crate.
2. `stillflow-plan` contains deterministic logical schemas, expressions, rules,
   logical plans, validation, and canonicalization. It must not contain physical
   engine objects.
3. Apache Arrow 59 is the bounded execution interchange protocol. Prefer the
   focused `arrow-array` and `arrow-schema` crates over the `arrow` meta crate.
4. Public batches cross execution boundaries in a versioned `BatchEnvelope`; raw
   `RecordBatch` values remain an internal payload. This contract is introduced
   in its own delivery node.
5. Polars is the one canonical cleaning and transformation executor.
6. DuckDB owns bounded preview SQL, federation, joins, and temporary
   materialization. It must not define a second cleaning-rule language.
7. SQLite stores control-plane metadata. Immutable Parquet partitions store
   materialized tabular snapshots. Neither format is an in-memory domain model.
8. Connectors expose capabilities and bounded streams. They must not expose
   Polars `DataFrame` or DuckDB connection objects.
9. AI may interpret metadata and results; it must never become the bulk-data
   execution path.
10. Secrets must not appear in domain objects, logs, events, fixtures, snapshots,
    or serialized API payloads. Persist `CredentialRef` values only.
11. Do not add `clone`, `Arc`, or `Box` merely to suppress ownership errors.
    Explain material ownership choices in the Implementation Contract.
12. Do not change frontend layout, components, CSS, or design tokens unless the
    issue explicitly requests a UI change.

## Contract and risk gates

| Risk | Required flow |
| --- | --- |
| `risk:low` | implementation -> CI |
| `risk:medium` | contract note -> implementation -> review -> CI |
| `risk:high` | frozen contract -> implementation -> architecture review -> CI |

Treat work as `risk:high` when it changes a public trait, core domain type,
logical schema, expression/rule AST, plan serialization, Arrow stream or
envelope, cancellation/backpressure, secret handling, persistence format, or
three or more crates.

Breaking a merged public contract is allowed only when all of these are true:

- an open issue states the migration and non-goals;
- a frozen contract names every public breaking change;
- the PR links both documents and reports downstream compile fixes;
- compatibility shims are either implemented or explicitly rejected.

Issue #23 is the authorization to break the contracts merged in #5 for stable
`LogicalSchema`, typed `Expr`, serializable rules, and plan DAGs. `BatchEnvelope`
is intentionally deferred to a later contract.

## Branch and PR conventions

- Use `agent/issue-NNN-short-description` from the latest accepted base.
- Prefer one issue per branch. A docs-only governance PR may group tightly coupled
  contract issues when its body lists every issue and contains no implementation.
- Keep commits atomic and make a stacked PR's base branch explicit.
- A medium/high-risk PR must link its contract under `docs/issues/`.
- Do not rewrite or delete another contributor's changes to make a branch clean.

## Required verification

Run the checks relevant to touched files and report any unavailable tool as an
environment limitation, never as a pass:

```bash
cd backend && cargo fmt --all -- --check
cd backend && cargo clippy --workspace --all-targets -- -D warnings
cd backend && cargo test --workspace
npm run typecheck
npm run build
```

For contract or architecture-only changes, also verify links, issue numbers,
dependency arrows, and that every acceptance criterion is objectively testable.

## Completion report

Every implementation handoff must list:

```text
Modified files
New dependencies
Public API changes
unwrap / expect usage
TODO items
Test results
Contract deviations
Remaining risks
Branch name
Commit SHA
```

Stop and return to contract review if implementation needs an unlisted public
contract change, a new execution engine responsibility, an unbounded operation,
or access to raw credentials.
