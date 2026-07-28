# StillFlow — Agent Instructions

Permanent constraints for Sol (review / contract) and Composer (implementation).
Detailed architecture: [`docs/data-ingestion-architecture.md`](docs/data-ingestion-architecture.md).
Development workflow: [`docs/development/ai-development-workflow.md`](docs/development/ai-development-workflow.md).

## Frozen engineering rules

1. `stillflow-core` must not depend on Polars, DuckDB, SQLx, or Axum.
2. Apache Arrow `RecordBatch` is the structured data boundary between connectors and engines.
3. Prefer `arrow-array` and `arrow-schema`; do not add the full `arrow` meta crate.
4. `stillflow-connectors` defines only `SourceConnector`, `ConnectorCapabilities`, and `ConnectorRegistry` — no source-specific logic.
5. Third-party libraries (Polars, Calamine, object_store, SQLx, DuckDB) live only in their adapter crates or engine adapters.
6. Polars owns deterministic cleaning and transformation.
7. DuckDB owns preview SQL, sampling, federation, and temporary materialization.
8. AI interprets metadata and results; it must not become the execution path for large-file ingestion.
9. Secrets must never appear in domain objects, logs, events, or serialized API payloads. Use `CredentialRef` only.
10. Do not add `clone`, `Arc`, or `Box` to bypass ownership problems without justification in the Implementation Contract.
11. Do not modify public traits or core domain types unless the Implementation Contract explicitly authorizes it.
12. Do not change frontend layout, CSS, or design tokens unless explicitly requested.

## Crate dependency direction

```text
stillflow-api → stillflow-engine → stillflow-connectors → stillflow-core
```

No cycles. `stillflow-core` depends on no other workspace crate.

## Risk routing

| Label | Flow |
| --- | --- |
| `risk:low` | Composer → CI |
| `risk:medium` | Composer → Sol review → Composer fix |
| `risk:high` | Sol contract → Composer → Sol review → Composer fix → Sol final |

Auto-upgrade to `risk:high` when touching: `SourceConnector`, core domain model, Arrow streams, cancellation/timeouts/backpressure, secrets/PII, Dataset/Snapshot/Checkpoint lifecycle, Polars/DuckDB boundary, or three or more crates.

## Branch and PR conventions

- Branch: `agent/issue-NNN-short-description` (one issue, one branch, one worktree).
- Composer implements on the branch; Sol reviews the diff only.
- PR body must reference the Implementation Contract when `risk:medium` or `risk:high`.
- `main` is the only confirmed source of truth after merge.

## Composer completion report

After implementation, report:

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

## Stop conditions

If implementation requires changing `SourceConnector`, `ReadRequest`, `BatchStream`, core error types, or other frozen contracts **without** an authorized contract update — stop and return to Sol.
