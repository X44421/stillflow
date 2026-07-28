# Stillflow Backend

Rust workspace for the DataCleaner OS data ingestion backend. See
`docs/data-ingestion-architecture.md` for the full architecture.

## Crates

Dependency direction is strictly one-way (`api -> engine -> connectors ->
core`); circular dependencies are not allowed.

| Crate | Responsibility |
| --- | --- |
| `stillflow-core` | Domain model and shared data contracts (sessions, objects, datasets, snapshots, schema descriptors, typed errors). Apache Arrow is the interchange protocol at its boundary. Depends on no other workspace crate. |
| `stillflow-connectors` | The single connector contract (discovery, inspection, preview, streaming reads, checkpoints) and the connector registry. |
| `stillflow-engine` | Ingestion execution and orchestration inside a session: drives connectors, streams record batches, registers datasets/snapshots, records auditable events. |
| `stillflow-api` | External API surface that translates requests into engine calls and serializes results. Owns no ingestion logic. |

## Policy

- Shared dependencies are declared once in the root `Cargo.toml` under
  `[workspace.dependencies]` and inherited with `<dep>.workspace = true`.
- Exact dependency versions (including Arrow) are pinned by the committed
  `Cargo.lock`; do not delete it.
- Edition and toolchain policy live in the workspace `Cargo.toml` and the
  repo-root `rust-toolchain.toml` (stable + rustfmt + clippy).

## Checks

Run from this directory:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

All three also run in CI (`.github/workflows/ci.yml`), alongside the
existing frontend build.
